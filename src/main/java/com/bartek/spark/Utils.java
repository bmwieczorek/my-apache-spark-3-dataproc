package com.bartek.spark;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Utils {

    public static Schema getSchema(String schemaFilePath) {
        try (InputStream inputStream = SparkVtdJavaApp.class.getClassLoader().getResourceAsStream(schemaFilePath)) {
            return getSchema(inputStream);
        } catch (IOException e) {
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

    public static ByteBuffer doubleToByteBuffer(double d) {
        BigDecimal bigDecimal = BigDecimal.valueOf(d).setScale(9, RoundingMode.UNNECESSARY);
        BigInteger bigInteger = bigDecimal.unscaledValue();
        return ByteBuffer.wrap(bigInteger.toByteArray());
    }

    public static BigDecimal byteBufferToBigDecimal(ByteBuffer byteBuffer) {
        ByteBuffer bb = clone(byteBuffer);
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        BigInteger bigInteger = new BigInteger(bytes);
        return new BigDecimal(bigInteger, 9);
    }

    public static ByteBuffer clone(ByteBuffer original) {
        ByteBuffer clone = ByteBuffer.allocate(original.capacity());
        original.rewind(); //copy from the beginning
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }

    public static byte[] readBytesFromFile(String filePath) {
        try {
            return Files.readAllBytes(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
