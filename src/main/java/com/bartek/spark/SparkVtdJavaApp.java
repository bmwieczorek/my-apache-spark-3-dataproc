package com.bartek.spark;

import com.bartek.spark.parser.VtdXmlGenericRecordParser;
import com.bartek.spark.parser.VtdXmlParser;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bartek.spark.Utils.*;


public class SparkVtdJavaApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkVtdJavaApp.class);

    public static void main(String[] args) {
        Map<String, String> argsMap = Arrays.stream(args).collect(Collectors.toMap(arg -> arg.split("=")[0], arg -> arg.split("=")[1]));
        String appName = SparkVtdJavaApp.class.getSimpleName();
        SparkConf sparkConf = new SparkConf().setAppName(appName);
        if (isLocal()) {
            sparkConf.setMaster("local[*]");
            String projectId = System.getenv("GCP_PROJECT"); // replace with your project id
            argsMap.put("projectId", projectId);
            argsMap.put("bucket", projectId + "-bartek-dataproc");
            argsMap.put("sourceTable", "bartek_person.bartek_person_table");
            argsMap.put("targetTable", "bartek_person.bartek_person_spark");
            String localFilePath = "target/myRecord.snappy.avro";
            argsMap.put("path", localFilePath);
            createAvroFile(localFilePath);
        }
        LOGGER.info("argsMap={}", argsMap);

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> dataDS = spark.read().format("avro")
                .load(argsMap.get("path"));
        dataDS.printSchema();
        dataDS.show();

        Schema outputSchema = getSchema("myRecord.avsc");
        StructType outputStructType = (StructType) SchemaConverters.toSqlType(outputSchema).dataType();

        Dataset<Row> dataDS4 = dataDS.mapPartitions((MapPartitionsFunction<Row, Row>) inputIterator -> {
            LOGGER.info("[MapPartitions] setup"); // executed once per partition
            VtdXmlParser vtdXmlParser = new VtdXmlParser(outputSchema);
            VtdXmlGenericRecordParser vtdXmlGenericRecordParser = new VtdXmlGenericRecordParser(outputSchema);

            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return inputIterator.hasNext();
                }

                @Override
                public Row next() {
                    Row inputRow = inputIterator.next();
                    byte[] bytes = inputRow.getAs("myRequiredBytes");
//                    Row outputRow = vtdXmlParser.parseBytes(bytes);
                    GenericRecord record = vtdXmlGenericRecordParser.parseBytes(bytes);
                    Row outputRow = GenericRecordToRowConverter.convert(record);
                    LOGGER.info("[MapPartitions] processing {}, {}", inputRow, outputRow); // executed for each row in partition
                    return outputRow;
                }
            };
        }, RowEncoder.apply(outputStructType));

        dataDS4.printSchema();
        dataDS4.show();

        dataDS4.write().format("bigquery")
                .option("writeMethod", "indirect")
                .option("temporaryGcsBucket", argsMap.get("bucket"))
                .option("intermediateFormat", "avro")
                .mode(SaveMode.Append)
                .save(argsMap.get("targetTable"));

        spark.stop();
    }

    private static void createAvroFile(String path) {
        Schema timestampMicrosLogicalType = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
        Schema dateLogicalType = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
        Schema decimal38LogicalType = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));
        Schema intputSchema = SchemaBuilder.record("myRecord")
                .fields()
                .requiredInt("myRequiredInt")
                .requiredString("myRequiredString")
                .optionalString("myOptionalString")
                .nullableString("myNullableString", "myNullableStringDefaultValue")
                .requiredBoolean("myRequiredBoolean")
                .requiredBytes("myRequiredBytes")
                .name("myBytesDecimal").type(decimal38LogicalType).noDefault()
                .name("myRequiredTimestamp").type(timestampMicrosLogicalType).noDefault() // needs to be timestamp_micros (not timestamp_millis)
                .name("myOptionalTimestamp").type().optional().type(timestampMicrosLogicalType)
                .name("myRequiredDate").doc("Expiration date field").type(dateLogicalType).noDefault()
                .name("myRequiredArrayLongs").type().array().items().longType().noDefault()
                .name("myRequiredSubRecord").type(SchemaBuilder.record("myRequiredSubRecordType").fields().requiredDouble("myRequiredDouble").requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                .name("myOptionalSubRecord").type().optional().record("myOptionalSubRecordType").fields().requiredFloat("myRequiredFloat").requiredBoolean("myRequiredBoolean").endRecord()
                .name("myNullableSubRecord").type().nullable().record("myNullableSubRecordType").fields().requiredLong("myRequiredLong").requiredBoolean("myRequiredBoolean").endRecord().noDefault()
                .name("myOptionalArraySubRecords").type().nullable().array().items(
                        SchemaBuilder.record("myOptionalArraySubRecord").fields().requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                .endRecord();
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(intputSchema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());
        try {
            dataFileWriter.create(intputSchema, new File(path));
            for (int i = 1; i <= 1; i++) {
                GenericRecord record = createGenericRecord(intputSchema);
                dataFileWriter.append(record);
            }
            dataFileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static GenericRecord createGenericRecord(Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("myRequiredInt", 123);
        record.put("myRequiredString", "abc");
        record.put("myRequiredBoolean", true);
        byte[] bytes = readBytesFromFile("src/test/resources/myRecord.xml");
        record.put("myRequiredBytes", ByteBuffer.wrap(bytes));
        record.put("myBytesDecimal", doubleToByteBuffer(1.23d));
        record.put("myRequiredTimestamp", System.currentTimeMillis() * 1000); // needs to be timestamp_micros (not timestamp_millis)
        record.put("myRequiredDate", (int) new Date(System.currentTimeMillis()).toLocalDate().toEpochDay());
        record.put("myRequiredArrayLongs", Arrays.asList(1L, 2L, 3L));
        Schema myRequiredSubRecordSchema = schema.getField("myRequiredSubRecord").schema();
        GenericRecord myRequiredSubRecord = new GenericData.Record(myRequiredSubRecordSchema);
        myRequiredSubRecord.put("myRequiredDouble", 1.0d);
        myRequiredSubRecord.put("myRequiredBoolean", false);
        record.put("myRequiredSubRecord", myRequiredSubRecord);
        Schema myOptionalSubRecordSchema = unwrapSchema(schema.getField("myOptionalSubRecord").schema());
        GenericRecord myOptionalSubRecord = new GenericData.Record(myOptionalSubRecordSchema);
        myOptionalSubRecord.put("myRequiredFloat", 2.0f);
        myOptionalSubRecord.put("myRequiredBoolean", true);
        record.put("myOptionalSubRecord", myOptionalSubRecord);
        Schema myNullableSubRecordSchema = unwrapSchema(schema.getField("myNullableSubRecord").schema());
        GenericRecord myNullableSubRecord = new GenericData.Record(myNullableSubRecordSchema);
        myNullableSubRecord.put("myRequiredLong", 3L);
        myNullableSubRecord.put("myRequiredBoolean", false);
        record.put("myNullableSubRecord", myNullableSubRecord);
        Schema myOptionalArraySubRecords = schema.getField("myOptionalArraySubRecords").schema();
        Schema arraySubRecordSchema = unwrapSchema(myOptionalArraySubRecords).getElementType();
        GenericRecord mySubRecord1 = new GenericData.Record(arraySubRecordSchema);
        mySubRecord1.put("myRequiredBoolean", true);
        GenericRecord mySubRecord2 = new GenericData.Record(arraySubRecordSchema);
        mySubRecord2.put("myRequiredBoolean", false);
        record.put("myOptionalArraySubRecords", Arrays.asList(mySubRecord1, mySubRecord2));
        return record;
    }

    private static boolean isLocal() {
        String osName = System.getProperty("os.name").toLowerCase();
        return osName.contains("mac") || osName.contains("windows");
    }
}
