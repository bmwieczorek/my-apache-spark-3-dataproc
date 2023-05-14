package com.bartek.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class SparkJavaApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkJavaApp.class);

    public static void main(String[] args) {
        Map<String, String> argsMap = Arrays.stream(args).collect(Collectors.toMap(arg -> arg.split("=")[0], arg -> arg.split("=")[1]));
        String appName = SparkJavaApp.class.getSimpleName();
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        if (isLocal()) {
            sparkConf.setMaster("local[*]");
            String projectId = System.getenv("GCP_PROJECT"); // replace with your project id
            argsMap.put("projectId", projectId);
            argsMap.put("bucket", projectId + "-bartek-dataproc");
            argsMap.put("sourceTable", "bartek_person.bartek_person_table");
            argsMap.put("targetTable", "bartek_person.bartek_person_spark");
        }
        LOGGER.info("argsMap={}", argsMap);

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> dataDS = spark.read().format("avro")
                .load("gs://" + argsMap.get("bucket") + "/myRecord.snappy.avro");
        dataDS.printSchema();

        Dataset<Row> refDS = spark.read().format("bigquery")
                .option("viewsEnabled", "true")
                .option("materializationDataset", "bartek_person")
                .load("SELECT distinct name, UPPER(name) as uname FROM " + argsMap.get("sourceTable"));

        Map<String, String> refMap = Arrays.stream(cast(refDS.collect())).collect(Collectors.toMap(row -> (String) (row.get(0)), row -> (String) (row.get(1))));

        Broadcast<Map<String, String>> broadcast = spark.sparkContext().broadcast(refMap, scala.reflect.ClassTag$.MODULE$.apply(refMap.getClass()));

        // or alternatively
//        Broadcast<Map<String, String>> broadcast = new JavaSparkContext(spark.sparkContext()).broadcast(refMap);


        UserDefinedFunction getCountryUDF = udf((UDF1<String, String>) (name -> getCountry(name, broadcast)), DataTypes.StringType);

        Dataset<Row> dataDS2 = dataDS.withColumn("uname", getCountryUDF.apply(col("name")));
        dataDS2.printSchema();

        Dataset<Row> dataDS3 = dataDS2.map(
                (MapFunction<Row, Row>) row -> {
                    String name = row.getAs("name");
                    byte[] body = row.getAs("body");
                    String uname = row.getAs("uname");
                    Row newRow = RowFactory.create(name, new String(body), uname);
                    LOGGER.info("processing {}", newRow);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return newRow;
                }, RowEncoder.apply(new StructType(Arrays.stream(dataDS2.schema().fields()).map(structField -> "body".equals(structField.name()) ? new StructField("body", StringType, true, Metadata.empty()) : structField).toArray(StructField[]::new)))
            );

        // or alternatively

//        Dataset<Row> dataDS3 = dataDS2.map(
//                (MapFunction<Row, Tuple3<String, String, String>>) row -> {
//                    String name = row.getAs("name");
//                    byte[] body = row.getAs("body");
//                    String uname = row.getAs("uname");
//                    Tuple3<String, String, String> tuple3 = new Tuple3<>(name, new String(body), uname);
//                    LOGGER.info("processing {}", tuple3);
//                    try {
//                        Thread.sleep(100);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                    return tuple3;
//                }, Encoders.tuple(Encoders.STRING(), Encoders.STRING(), Encoders.STRING())
//            ).toDF(Arrays.stream(dataDS2.schema().fields()).map(StructField::name).toArray(String[]::new));


        dataDS3.printSchema();

        dataDS3.write().format("bigquery")
                .option("writeMethod", "indirect")
                .option("temporaryGcsBucket", argsMap.get("bucket"))
                .mode(SaveMode.Append)
                .save(argsMap.get("targetTable"));

        spark.stop();
    }

    private static String getCountry(String name, Broadcast<Map<String, String>> broadcast) {
        Map<String, String> value = broadcast.getValue();
        return value.getOrDefault(name, "UNKNOWN");
    }

    private static boolean isLocal() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.contains("mac") || osName.contains("windows");
    }

    private static Row[] cast(Object o) {
        // since DataFrame.collect() returns generic scala Array[T] (not Array[Row]) we need to add this cast
        return (Row[]) o;
    }
}
