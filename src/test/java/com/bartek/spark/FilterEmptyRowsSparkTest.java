package com.bartek.spark;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FilterEmptyRowsSparkTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterEmptyRowsSparkTest.class);

    @Test
    public void test() {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        // working alternative
//        List<String> list = Arrays.asList("a", "b");
//        Seq<String> seq = convertJavaListToScalaSeq(list);
//        Dataset<Row> rowDataset = spark.implicits().localSeqToDatasetHolder(seq, Encoders.STRING()).toDF();


        List<Row> rows = Arrays.asList(RowFactory.create("A"), RowFactory.create("B"));
        StructType structType = new StructType(new StructField[]{new StructField("name", DataTypes.StringType, false, Metadata.empty())});

        Dataset<Row> rowDataset = spark.createDataset(convertJavaListToScalaSeq(rows), RowEncoder.apply(structType));
        rowDataset.show();

        // requires junit to implement serializable
//        Dataset<Row> rowDataset2 = rowDataset.mapPartitions((MapPartitionsFunction<Row, Row>) input -> new Iterator<>() {
//            @Override
//            public boolean hasNext() {
//                return input.hasNext();
//            }
//
//            @Override
//            public Row next() {
//                Row inputRow = input.next();
//                String value = inputRow.getString(0);
//                Row outputRow = "A".equals(value) ? RowFactory.create() : inputRow;
//                LOGGER.info("inputRow={}, outputRow={}", inputRow, outputRow);
//                return outputRow;
//            }
//        }, RowEncoder.apply(structType));

        Dataset<Row> rowDataset2 = rowDataset.mapPartitions(new MyMapPartitions(), RowEncoder.apply(structType));

        Dataset<Row> rowDataset3= rowDataset2.filter((FilterFunction<Row>) row -> row.length() != 0);

        rowDataset3.show();
        long count = rowDataset3.count();
        Assertions.assertEquals(1, count);
    }

    static class MyMapPartitions implements MapPartitionsFunction<Row, Row> {
        @Override
        public Iterator<Row> call(Iterator<Row> input) {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return input.hasNext();
                }

                @Override
                public Row next() {
                    Row inputRow = input.next();
                    String value = inputRow.getString(0);
                    Row outputRow = "A".equals(value) ? RowFactory.create() : inputRow;
                    LOGGER.info("inputRow={}, outputRow={}", inputRow, outputRow);
                    return outputRow;
                }
            };
        }
    }

    private static <T> Seq<T> convertJavaListToScalaSeq(List<T> strings) {
        return JavaConverters.asScalaIteratorConverter(strings.iterator()).asScala().toSeq();
    }
}
