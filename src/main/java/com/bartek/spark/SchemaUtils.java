package com.bartek.spark;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class SchemaUtils {

    public static Schema getSchema(String schemaFilePath) {
        try (InputStream inputStream = SparkVtdJavaApp.class.getClassLoader().getResourceAsStream(schemaFilePath)) {
            return getSchema(inputStream);
        } catch(IOException e){
            throw new IllegalStateException("Unable to parse avro schema: " + schemaFilePath + " due to: " + e);
        }
    }

    public static Schema getSchema(InputStream inputStream) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        try (InputStream is = inputStream) {
            return parser.parse(is);
        }
    }

    public static Schema unwrapSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> types = schema.getTypes();
            return types.get(0).getType() == Schema.Type.NULL ? types.get(1) : types.get(0);
        }
        return schema;
    }
}
