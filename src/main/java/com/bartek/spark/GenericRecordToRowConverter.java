package com.bartek.spark;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.List;

import static com.bartek.spark.Utils.unwrapSchema;

public class GenericRecordToRowConverter {
    public static Row convert(GenericRecord record) {
        Schema schema = record.getSchema();
        List<Schema.Field> fields = schema.getFields();
        Object[] data = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Schema fieldSchema = unwrapSchema(fields.get(i).schema());
            data[i] = convert(record.get(i), fieldSchema);
        }
        return RowFactory.create(data);
    }

    @SuppressWarnings({"unchecked", "DuplicateBranchesInSwitch"})
    private static Object convert(Object obj, Schema schema) {
        Schema.Type type = schema.getType();
        switch (type) {
            case INT:
            case STRING:
            case LONG:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case BYTES:
                return obj;
            case RECORD: {
                GenericRecord record = (GenericRecord) obj;
                List<Schema.Field> fields = record.getSchema().getFields();
                Object[] values = new Object[fields.size()];
                for (int i = 0; i < fields.size(); i++) {
                    Schema.Field field = fields.get(i);
                    values[i] = convert(record.get(i), unwrapSchema(field.schema()));
                }
                return RowFactory.create(values);
            }
            case ARRAY: {
                Schema elementSchema = unwrapSchema(schema.getElementType());
                Schema.Type elementSchemaType = elementSchema.getType();
                if (elementSchemaType == Schema.Type.RECORD) {
                    GenericData.Array<GenericRecord> recordArray = (GenericData.Array<GenericRecord>) obj;
                    Row[] rows = new Row[recordArray.size()];
                    for (int i = 0; i < recordArray.size(); i++) {
                        GenericRecord record = recordArray.get(i);
                        Row convert = (Row) convert(record, record.getSchema());
                        rows[i] = convert;
                    }
                    return rows;
                } else {
                    switch (elementSchemaType) {
                        case INT:
                            return ((List<Integer>) obj).toArray(Integer[]::new);
                        case STRING:
                            return ((List<String>) obj).toArray(String[]::new);
                        case LONG:
                            return ((List<Long>) obj).toArray(Long[]::new);
                        case BOOLEAN:
                            return ((List<Boolean>) obj).toArray(Boolean[]::new);
                        case FLOAT:
                            return ((List<Float>) obj).toArray(Float[]::new);
                        case DOUBLE:
                            return ((List<Double>) obj).toArray(Double[]::new);
                        case BYTES:
                            return obj;
                        default:
                            throw new UnsupportedOperationException("Unsupported array type " + type + ", " + schema.getLogicalType());
                    }
                }
            }
            default:
                throw new UnsupportedOperationException("Unsupported type " + type + ", " + schema.getLogicalType());
        }
    }
}
