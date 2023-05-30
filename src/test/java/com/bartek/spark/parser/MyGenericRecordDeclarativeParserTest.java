package com.bartek.spark.parser;

import com.bartek.spark.Utils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;

public class MyGenericRecordDeclarativeParserTest {
    @Test
    public void shouldParseXmlFile() {
        Schema schema = Utils.getSchema("myRecord.avsc");
        VtdXmlGenericRecordParser vtdXmlParser = new VtdXmlGenericRecordParser(schema);
        // given
        String xmlFilePath = "src/test/resources/myRecord.xml";
        // when
        GenericRecord record = vtdXmlParser.parseFile(xmlFilePath);
//        GenericRecord row = vtdXmlParser.parseFile(xmlFilePath);
        // then
        runAssertions(record);
//        runAssertions(row, schema);
    }

    private static void runAssertions(GenericRecord record) {
        Assertions.assertEquals(1234, record.get("myRequiredInt"));
        Assertions.assertEquals("abc", record.get("myRequiredString"));
        Assertions.assertEquals("ABCDEF", new String((byte[]) record.get("myRequiredBytes")));
        Assertions.assertEquals("2023-05-29", record.get("myRequiredDate").toString());

//        Assertions.assertEquals(BigDecimal.valueOf(3.45).setScale(9, RoundingMode.UNNECESSARY), byteBufferToBigDecimal((ByteBuffer) record.get("myBytesDecimal"))));
        Assertions.assertEquals(BigDecimal.valueOf(34.5).setScale(9, RoundingMode.UNNECESSARY), record.get("myBytesDecimal"));
        Assertions.assertEquals(Arrays.asList(11L, 12L, 13L), record.get("myRequiredArrayLongList"));
        Assertions.assertEquals(Arrays.asList(21L, 22L), record.get("myRequiredArrayLongs"));
        GenericRecord myRequiredArrayLongsRow2 = (GenericRecord) record.get("myRequiredArrayLongs2");
        Assertions.assertEquals(Arrays.asList(21L, 22L), myRequiredArrayLongsRow2.get(0));

        GenericRecord myRequiredSubRecord = (GenericRecord) record.get("myRequiredSubRecord");
        Assertions.assertEquals(30.0d, myRequiredSubRecord.get("myRequiredDouble"));
        Assertions.assertFalse((Boolean) myRequiredSubRecord.get("myRequiredBoolean"));

        GenericRecord myOptionalSubRecord = (GenericRecord) record.get("myOptionalSubRecord");
        Assertions.assertEquals(40.0f, myOptionalSubRecord.get("myRequiredFloat"));
        Assertions.assertTrue((Boolean) myOptionalSubRecord.get("myRequiredBoolean"));

        GenericRecord myNullableSubRecord = (GenericRecord) record.get("myNullableSubRecord");
        Assertions.assertEquals(50, myNullableSubRecord.get("myRequiredInt"));
        Assertions.assertTrue((Boolean) myNullableSubRecord.get("myRequiredBoolean"));

        @SuppressWarnings("unchecked") List<Row> myOptionalArraySubRecordList = (List<Row>) record.get("myOptionalArraySubRecordList");
        Assertions.assertEquals(2, myOptionalArraySubRecordList.size());
        Assertions.assertEquals(60.0d, ((GenericRecord) myOptionalArraySubRecordList.get(0)).get("myRequiredDouble"));
        Assertions.assertTrue((Boolean) ((GenericRecord) myOptionalArraySubRecordList.get(0)).get("myRequiredBoolean"));
        Assertions.assertEquals(70.0d, ((GenericRecord) myOptionalArraySubRecordList.get(1)).get("myRequiredDouble"));
        Assertions.assertFalse((Boolean) ((GenericRecord) myOptionalArraySubRecordList.get(1)).get("myRequiredBoolean"));

        @SuppressWarnings("unchecked") List<Row> myOptionalArraySubRecords = (List<Row>) record.get("myOptionalArraySubRecords");
        Assertions.assertEquals(2, myOptionalArraySubRecords.size());
        Assertions.assertEquals(80.0f, ((GenericRecord) myOptionalArraySubRecords.get(0)).get("myRequiredFloat"));
        Assertions.assertFalse((Boolean) ((GenericRecord) myOptionalArraySubRecords.get(0)).get("myRequiredBoolean"));
        Assertions.assertEquals(90.0f, ((GenericRecord) myOptionalArraySubRecords.get(1)).get("myRequiredFloat"));
        Assertions.assertTrue((Boolean) ((GenericRecord) myOptionalArraySubRecords.get(1)).get("myRequiredBoolean"));

        GenericRecord myOptionalArraySubRecords2 = (GenericRecord) record.get("myOptionalArraySubRecords2");
        @SuppressWarnings("unchecked") List<Row> myOptionalArraySubRecordList2 = (List<Row>) myOptionalArraySubRecords2.get("myOptionalArraySubRecordList");
        Assertions.assertEquals(2, myOptionalArraySubRecordList2.size());
        Assertions.assertEquals(80.0f, ((GenericRecord) myOptionalArraySubRecordList2.get(0)).get("myRequiredFloat"));
        Assertions.assertFalse((Boolean) ((GenericRecord) myOptionalArraySubRecordList2.get(0)).get("myRequiredBoolean"));
        Assertions.assertEquals(90.0f,((GenericRecord) myOptionalArraySubRecordList2.get(1)).get("myRequiredFloat"));
        Assertions.assertTrue((Boolean) ((GenericRecord) myOptionalArraySubRecordList2.get(1)).get("myRequiredBoolean"));
    }
}
