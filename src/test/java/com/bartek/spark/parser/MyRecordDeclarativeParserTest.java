package com.bartek.spark.parser;

import com.bartek.spark.GenericRecordToRowConverter;
import com.bartek.spark.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.bartek.spark.Utils.unwrapSchema;

public class MyRecordDeclarativeParserTest {
    @Test
    public void shouldParseXmlFile() {
        Schema schema = Utils.getSchema("myRecord.avsc");
        VtdXmlParser vtdXmlParser = new VtdXmlParser(schema);
        // given
        String xmlFilePath = "src/test/resources/myRecord.xml";
        // when
//        GenericRecord record = vtdXmlParser.parseFile(xmlFilePath);
        Row row = vtdXmlParser.parseFile(xmlFilePath);
        // then
//        runAssertions(record);
        runAssertions(row, schema);
    }

    @Test
    public void shouldParseXmlFile2() {
        Schema schema = Utils.getSchema("myRecord.avsc");
        VtdXmlGenericRecordParser vtdXmlParser = new VtdXmlGenericRecordParser(schema);
        // given
        String xmlFilePath = "src/test/resources/myRecord.xml";
        // when
        GenericRecord record = vtdXmlParser.parseFile(xmlFilePath);
        Row row = GenericRecordToRowConverter.convert(record);
        // then
//        runAssertions(record);
        runAssertions(row, schema);
    }

    private static void runAssertions(Row row, Schema schema) {
        Assertions.assertEquals(1234, row.getInt(schema.getField("myRequiredInt").pos()));
        Assertions.assertEquals("abc", row.getString(schema.getField("myRequiredString").pos()));
        Assertions.assertEquals("ABCDEF", new String((byte[]) row.get(schema.getField("myRequiredBytes").pos())));
        Assertions.assertEquals("2023-05-29", row.getDate(schema.getField("myRequiredDate").pos()).toString());
//        Assertions.assertEquals(BigDecimal.valueOf(3.45).setScale(9, RoundingMode.UNNECESSARY), byteBufferToBigDecimal((ByteBuffer) row.get(schema.getField("myBytesDecimal").pos())));
        Assertions.assertEquals(BigDecimal.valueOf(34.5).setScale(9, RoundingMode.UNNECESSARY), row.get(schema.getField("myBytesDecimal").pos()));
        Assertions.assertArrayEquals(new Long[]{ 11L, 12L, 13L }, (Long[]) row.get(schema.getField("myRequiredArrayLongList").pos()));
        Assertions.assertArrayEquals(new Long[]{ 21L, 22L }, (Long[]) row.get(schema.getField("myRequiredArrayLongs").pos()));
        Row myRequiredArrayLongsRow2 = (Row) row.get(schema.getField("myRequiredArrayLongs2").pos());
        Assertions.assertArrayEquals(new Long[]{ 21L, 22L }, (Long[]) myRequiredArrayLongsRow2.get(0));

        Row myRequiredSubRecordRow = (Row) row.get(schema.getField("myRequiredSubRecord").pos());
        Schema myRequiredSubRecordRowSchema = schema.getField("myRequiredSubRecord").schema();
        Assertions.assertEquals(30.0d, myRequiredSubRecordRow.getDouble(myRequiredSubRecordRowSchema.getField("myRequiredDouble").pos()));
        Assertions.assertFalse(myRequiredSubRecordRow.getBoolean(myRequiredSubRecordRowSchema.getField("myRequiredBoolean").pos()));

        Row myOptionalSubRecordRow = (Row) row.get(schema.getField("myOptionalSubRecord").pos());
        Schema myOptionalSubRecordRowSchema = unwrapSchema(schema.getField("myOptionalSubRecord").schema());
        Assertions.assertEquals(40.0f, myOptionalSubRecordRow.getFloat(myOptionalSubRecordRowSchema.getField("myRequiredFloat").pos()));
        Assertions.assertTrue(myOptionalSubRecordRow.getBoolean(myOptionalSubRecordRowSchema.getField("myRequiredBoolean").pos()));

        Row myNullableSubRecordRow = (Row) row.get(schema.getField("myNullableSubRecord").pos());
        Schema myNullableSubRecordRowSchema = unwrapSchema(schema.getField("myNullableSubRecord").schema());
        Assertions.assertEquals(50, myNullableSubRecordRow.getInt(myNullableSubRecordRowSchema.getField("myRequiredInt").pos()));
        Assertions.assertTrue(myNullableSubRecordRow.getBoolean(myNullableSubRecordRowSchema.getField("myRequiredBoolean").pos()));

        Row[] myOptionalArraySubRecordListRows = (Row[]) row.get(schema.getField("myOptionalArraySubRecordList").pos());
        Assertions.assertEquals(2, myOptionalArraySubRecordListRows.length);
        Schema myOptionalArraySubRecordListRowsSchema = unwrapSchema(schema.getField("myOptionalArraySubRecordList").schema());
        Schema myOptionalArraySubRecordListElementSchema = myOptionalArraySubRecordListRowsSchema.getElementType();
        Row myOptionalArraySubRecordListRowFirst = myOptionalArraySubRecordListRows[0];
        Assertions.assertEquals(60.0d, myOptionalArraySubRecordListRowFirst.getDouble(myOptionalArraySubRecordListElementSchema.getField("myRequiredDouble").pos()));
        Assertions.assertTrue(myOptionalArraySubRecordListRowFirst.getBoolean(myOptionalArraySubRecordListElementSchema.getField("myRequiredBoolean").pos()));
        Row myOptionalArraySubRecordListSecondRow = myOptionalArraySubRecordListRows[1];
        Assertions.assertEquals(70.0d, myOptionalArraySubRecordListSecondRow.getDouble(myOptionalArraySubRecordListElementSchema.getField("myRequiredDouble").pos()));
        Assertions.assertFalse(myOptionalArraySubRecordListSecondRow.getBoolean(myOptionalArraySubRecordListElementSchema.getField("myRequiredBoolean").pos()));

        Row[] myOptionalArraySubRecordsRow = (Row[]) row.get(schema.getField("myOptionalArraySubRecords").pos());
        Schema myOptionalArraySubRecordsRowSchema = unwrapSchema(schema.getField("myOptionalArraySubRecords").schema());
        Schema myOptionalArraySubRecordsElementSchema = myOptionalArraySubRecordsRowSchema.getElementType();
        Assertions.assertEquals(80.0f, myOptionalArraySubRecordsRow[0].getFloat(myOptionalArraySubRecordsElementSchema.getField("myRequiredFloat").pos()));
        Assertions.assertFalse(myOptionalArraySubRecordsRow[0].getBoolean(myOptionalArraySubRecordsElementSchema.getField("myRequiredBoolean").pos()));
        Assertions.assertEquals(90.0f, myOptionalArraySubRecordsRow[1].getFloat(myOptionalArraySubRecordsElementSchema.getField("myRequiredFloat").pos()));
        Assertions.assertTrue(myOptionalArraySubRecordsRow[1].getBoolean(myOptionalArraySubRecordsElementSchema.getField("myRequiredBoolean").pos()));

        Row myOptionalArraySubRecords2Row = (Row) row.get(schema.getField("myOptionalArraySubRecords2").pos());
        Schema myOptionalArraySubRecords2RowSchema = unwrapSchema(schema.getField("myOptionalArraySubRecords2").schema());
        Row[] myOptionalArraySubRecordList = (Row[]) myOptionalArraySubRecords2Row.get(myOptionalArraySubRecords2RowSchema.getField("myOptionalArraySubRecordList").pos());
        Schema myOptionalArraySubRecords2ElementRowSchema = unwrapSchema(myOptionalArraySubRecords2RowSchema.getField("myOptionalArraySubRecordList").schema());
        Schema myOptionalArraySubRecords2ElementRowSchemaElementType = myOptionalArraySubRecords2ElementRowSchema.getElementType();
        Assertions.assertEquals(80.0f, myOptionalArraySubRecordList[0].getFloat(myOptionalArraySubRecords2ElementRowSchemaElementType.getField("myRequiredFloat").pos()));
        Assertions.assertFalse(myOptionalArraySubRecordList[0].getBoolean(myOptionalArraySubRecords2ElementRowSchemaElementType.getField("myRequiredBoolean").pos()));
        Assertions.assertEquals(90.0f, myOptionalArraySubRecordList[1].getFloat(myOptionalArraySubRecords2ElementRowSchemaElementType.getField("myRequiredFloat").pos()));
        Assertions.assertTrue(myOptionalArraySubRecordList[1].getBoolean(myOptionalArraySubRecords2ElementRowSchemaElementType.getField("myRequiredBoolean").pos()));
    }
}
