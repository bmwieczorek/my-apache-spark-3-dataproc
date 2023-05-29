package com.bartek.spark.parser;

import com.ximpleware.*;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VtdXmlParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(VtdXmlParser.class);

    private static Function<String, ?> getConverterFunction(Type type, LogicalType logicalType) {
        switch (type) {
            case STRING: return s -> s;
            case INT: return s -> (logicalType == null ? Integer.parseInt(s) : new Date(Integer.parseInt(s) * 24 * 3600 * 1000L));
            case BOOLEAN: return Boolean::parseBoolean;
            case FLOAT: return Float::parseFloat;
            case DOUBLE: return Double::parseDouble;
            case LONG: return s -> (logicalType == null ? Long.parseLong(s) : new Timestamp(Long.parseLong(s)));
//            case BYTES: return s -> doubleToByteBuffer(Double.parseDouble(s));
            case BYTES: return s -> (logicalType == null ? s.getBytes() : BigDecimal.valueOf(Double.parseDouble(s)).setScale(9, RoundingMode.UNNECESSARY));
            default: throw new UnsupportedOperationException("Unsupported " + type + ", " + logicalType);
        }
    }

    private static ByteBuffer doubleToByteBuffer(Double d) {
        if (d == null) return null;
        BigDecimal bigDecimal = BigDecimal.valueOf(d).setScale(9, RoundingMode.UNNECESSARY);
        BigInteger bigInteger = bigDecimal.unscaledValue();
        return ByteBuffer.wrap(bigInteger.toByteArray());
    }

    private final List<Entry> mappingEntries;
    private final Schema schema;

    public VtdXmlParser(Schema schema) {
        this.mappingEntries = schema.getFields().stream().map(VtdXmlParser::convertToEntry).collect(Collectors.toList());
        this.schema = schema;
    }

//    public GenericRecord parseFile(String xmlFilePath) {
    public Row parseFile(String xmlFilePath) {
        VTDGen vtdGen = new VTDGen();
        vtdGen.parseFile(xmlFilePath, false);
        VTDNav nav = vtdGen.getNav();
        return parseVTDGen(nav, mappingEntries, schema);
    }

//    public GenericRecord parseXml(String xmlFilePath) {
    public Row parseXml(String xmlFilePath) {
        VTDGen vtdGen = new VTDGen();
        vtdGen.setDoc(xmlFilePath.getBytes());
        try {
            vtdGen.parse(false);
            VTDNav nav = vtdGen.getNav();
            return parseVTDGen(nav, mappingEntries, schema);
        } catch (ParseException e) {
            LOGGER.error("Failed to parse", e);
            throw new RuntimeException(e);
        }
    }

//    private GenericRecord parseVTDGen(VTDNav nav, List<Entry> mappingEntries, Schema outputRecordSchema) {
    private Row parseVTDGen(VTDNav nav, List<Entry> mappingEntries, Schema outputRecordSchema) {
//        GenericRecord outputRecord = new GenericData.Record(outputRecordSchema);
        List<Object> data = new ArrayList<>(schema.getFields().size());
        mappingEntries.forEach(entry -> {
            try {
                Schema schema = unwrapSchema(entry.schema);
                Type type = schema.getType();
                switch (type) {
                    case RECORD: {
//                        List<GenericRecord> records = processRecords(nav, entry, entry.schema);
                        Row[] subRows = processRecords(nav, entry, schema);
//                        GenericRecord subRecord = getFirstElementOrNull(records);
//                        outputRecord.put(entry.field, subRecord);
//                        data.add(subRecord);
                        data.addAll(Arrays.asList(subRows));
                        break;
                    }
                    case ARRAY: {
                        Schema elementSchema = unwrapSchema(schema.getElementType());
                        if (elementSchema.getType() == Type.RECORD) {
                            Row[] rows = processRecords(nav, entry, elementSchema);
                            data.add(rows);
                        } else {
                            Object elements = processValues(nav, entry, elementSchema);
                            data.add(elements);
                        }
//                        GenericData.Array<?> array = new GenericData.Array<>(schema, elements);
//                        outputRecord.put(entry.field, array);
                        break;
                    }
                    default: {
                        if (entry.clazz != null && CustomFieldParser.class.isAssignableFrom(entry.clazz)) {
                            Object value = parseField(nav, entry);
//                            outputRecord.put(entry.field, value);
                            data.add(value);
                        } else {
                            Object value = processValue(nav, entry, schema);
//                            outputRecord.put(entry.field, value);
                            data.add(value);
                        }
                    }
                }
            } catch (XPathParseException e) {
                LOGGER.error("Failed to parse value for entry " + entry, e);
            }
        });
//        return outputRecord;
        return RowFactory.create(data.toArray());
    }

    private Object processValue(VTDNav nav, Entry entry, Schema elementSchema) {
        List<String> values = extractValue(nav, entry);
        Function<String, ?> typeTransformationFunction = getConverterFunction(elementSchema.getType(), elementSchema.getLogicalType());
        String firstElementOrNull = getFirstElementOrNull(values);
        return firstElementOrNull == null ? null : typeTransformationFunction.apply(firstElementOrNull);
    }

    private Object processValues(VTDNav nav, Entry entry, Schema elementSchema) {
        List<String> values = extractValue(nav, entry);
        if (values.size() == 0) {
            return null;
        }
        Type type = elementSchema.getType();
        LogicalType logicalType = elementSchema.getLogicalType();
        Function<String, ?> typeTransformationFunction = getConverterFunction(type, logicalType);
        Stream<?> stream = values.stream().map(typeTransformationFunction);
        return toTypedArray(type, logicalType, stream);
    }

    @SuppressWarnings({"SuspiciousToArrayCall", "DuplicateBranchesInSwitch", "ConditionalExpressionWithIdenticalBranches"})
    private static Object toTypedArray(Type type, LogicalType logicalType, Stream<?> stream) {
        switch (type) {
            case STRING:
                return stream.toArray(String[]::new);
            case INT:
                return logicalType == null ? stream.toArray(Integer[]::new) : stream.toArray(Date[]::new);
            case BOOLEAN:
                return stream.toArray(Boolean[]::new);
            case FLOAT:
                return stream.toArray(Float[]::new);
            case DOUBLE:
                return stream.toArray(Double[]::new);
            case LONG:
                return logicalType == null ? stream.toArray(Long[]::new) : stream.toArray(Timestamp[]::new);
            case BYTES:
                return  logicalType == null ? stream.toArray(Byte[]::new) : stream.toArray(BigDecimal[]::new);
            default: throw new UnsupportedOperationException("Unsupported " + type + ", " + logicalType);
        }
    }

    private Row processRecord(VTDNav nav, Entry entry, Schema schema) {
        Row[] rows = processRecords(nav, entry, schema);
        return getFirstElementOrNull(rows);
    }


//    private List<GenericRecord> processRecords(VTDNav nav, Entry entry, Schema schema) {
    private Row[] processRecords(VTDNav nav, Entry entry, Schema schema) {
//        List<GenericRecord> records = new ArrayList<>();
        List<Row> rows = new ArrayList<>();
        String xpath = entry.xpath;
        AutoPilot ap = new AutoPilot();
        try {
            ap.selectXPath(xpath);
            ap.bind(nav);
            while (ap.evalXPath() > 0) { // requires a while loop, not if statement
//                GenericRecord record = parseVTDGen(nav, entry.children, schema);
                Row row = parseVTDGen(nav, entry.children, schema);
//                records.add(record);
                rows.add(row);
            }
        } catch (VTDException e) {
            LOGGER.error("Failed to process record for entry " + entry, e);
        } finally {
            ap.resetXPath();
        }
//        return records;
        return rows.toArray(Row[]::new);
    }

    private static List<String> extractValue(VTDNav nav, Entry entry) {
        String xpath = entry.xpath;
        List<String> results = new ArrayList<>();
        AutoPilot ap = new AutoPilot();
        try {
            ap.selectXPath(xpath);
            ap.bind(nav);

            if (xpath.startsWith("@") || xpath.contains("/@")) {
                int i;
                while ((i = ap.evalXPath()) > 0) { // requires a while loop, not if statement
                    String attrName = nav.toString(i);
                    int attrIdx = nav.getAttrVal(attrName);
                    if (attrIdx != -1) {
                        String attrValue = nav.toString(attrIdx);
                        LOGGER.info("AttrValue: {} => {}", xpath, attrValue);
                        results.add(attrValue);
                    }
                }
            } else {
                while (ap.evalXPath() != -1) { // requires a while loop, not if statement
                    long attrPosition = nav.getContentFragment();
                    if (attrPosition != -1) {
                        int textTokenIdx = nav.getText();
                        String text = nav.toString(textTokenIdx);
                        LOGGER.info("Text: {} => {}", xpath, text);
                        results.add(text);
                    }
                }
            }
        } catch (VTDException e) {
            LOGGER.error("Failed to extract value for entry " + entry, e);
        } finally {
            ap.resetXPath();
        }

        LOGGER.info("Values: {} => {}", xpath, results);
        return results;
    }

    private static Object parseField(VTDNav nav, Entry entry) throws XPathParseException {
        AutoPilot ap = new AutoPilot(nav);
        ap.selectXPath(entry.xpath);
        ap.bind(nav);
        try {
            CustomFieldParser fieldParser = (CustomFieldParser) entry.clazz.getDeclaredConstructor((Class<?>[]) null).newInstance();
            return fieldParser.parse(entry.field, entry.xpath, ap, nav);
        } catch (Exception e) {
            LOGGER.error("Failed to parse value for entry " + entry, e);
        } finally {
            ap.resetXPath();
        }
        return null;
    }

    public static class Entry implements Serializable {
        String field;
        String xpath;
        Schema schema;
        Class<?> clazz;
        List<Entry> children;

        public Entry(String field, String xpath, Schema schema, Class<?> clazz, List<Entry> children) {
            this.field = field;
            this.xpath = xpath;
            this.schema = schema;
            this.clazz = clazz;
            this.children = children;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            return Objects.equals(field, entry.field) && Objects.equals(xpath, entry.xpath) && schema == entry.schema && Objects.equals(clazz, entry.clazz) && Objects.equals(children, entry.children);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, xpath, schema, clazz, children);
        }

        @Override
        public String toString() {
            return "Entry{" + field + ',' + xpath + "," + schema + "," + clazz + "," + children + '}';
        }
    }

    private static Entry convertToEntry(Schema.Field field) {
        String parserClassName = field.getProp("class");
        Class<?> parserClass = parserClassName == null ? null : getParserClass(parserClassName);
        Schema schema = unwrapSchema(field.schema());
        List<Entry> childrenEntries;
        List<Schema.Field> childrenFields = Collections.emptyList();
        if (schema.getType() == Type.RECORD) {
            childrenFields = schema.getFields();
        } else {
            if (schema.getType() == Type.ARRAY) {
                Schema elementType = unwrapSchema(schema.getElementType());
                if (elementType.getType() == Type.RECORD) {
                    childrenFields = elementType.getFields();
                }
            }
        }
        childrenEntries = childrenFields.stream().map(VtdXmlParser::convertToEntry).collect(Collectors.toList());
        return new Entry(field.name(), field.getProp("xpath"), field.schema(), parserClass, childrenEntries);
    }

    private static Schema unwrapSchema(Schema schema) {
        if (schema.getType() == Type.UNION) {
            List<Schema> types = schema.getTypes();
            return types.get(0).getType() == Type.NULL ? types.get(1) : types.get(0);
        }
        return schema;
    }


    private static Class<?> getParserClass(String parserClassName) {
        Class<?> parserClass;
        try {
            parserClass = Class.forName(parserClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return parserClass;
    }

    private static <T> T getFirstElementOrNull(List<T> list) {
        return list != null && list.size() > 0 ? list.get(0) : null;
    }

    private static <T> T getFirstElementOrNull(T[] array) {
        return array != null && array.length > 0 ? array[0] : null;
    }
}
