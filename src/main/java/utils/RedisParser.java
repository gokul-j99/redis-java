package utils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class RedisParser {

    public static Object readEncodedValue(DataInputStream file, byte valueType) throws IOException {
        if (valueType == 0x00) {
            // String value
            int valueLength = file.readUnsignedByte();
            byte[] valueBytes = new byte[valueLength];
            file.readFully(valueBytes);
            return new String(valueBytes, StandardCharsets.UTF_8);
        } else {
            // Unknown value type
            return null;
        }
    }

    public static Object[] handleDatabaseSelector(DataInputStream file) throws IOException {
        int dbNumber = file.readUnsignedByte();
        // Skip resizedb field
        file.skipBytes(2);
        return new Object[] {null, null, null}; // Dummy return to match the expected Object[] return type
    }

    public static Object[] handleKeyValuePairWithExpirySeconds(DataInputStream file) throws IOException {
        int expiryTime = file.readInt();
        byte valueType = file.readByte();
        int keyLength = file.readUnsignedByte();
        byte[] keyBytes = new byte[keyLength];
        file.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        Object value = readEncodedValue(file, valueType);
        if (expiryTime > 0 && expiryTime < (int) (System.currentTimeMillis() / 1000)) {
            return new Object[] {null, null, 0};
        }
        return new Object[] {key, value, expiryTime};
    }

    public static Object[] handleKeyValuePairWithExpiryMilliseconds(DataInputStream file) throws IOException {
        long expiryTime = file.readLong();
        byte valueType = file.readByte();
        int keyLength = file.readUnsignedByte();
        byte[] keyBytes = new byte[keyLength];
        file.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        Object value = readEncodedValue(file, valueType);
        if (expiryTime > 0 && expiryTime < System.currentTimeMillis()) {
            return new Object[] {null, null, 0};
        }
        expiryTime = expiryTime / 1000;
        return new Object[] {key, value, expiryTime};
    }

    public static Object[] handleKeyValuePairWithoutExpiry(DataInputStream file, byte valueType) throws IOException {
        int keyLength = file.readUnsignedByte();
        byte[] keyBytes = new byte[keyLength];
        file.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);
        Object value = readEncodedValue(file, valueType);
        return new Object[] {key, value, 0};
    }

    public interface FieldHandler {
        Object[] handle(DataInputStream file) throws IOException;
    }

    public static final Map<Byte, FieldHandler> fieldHandlers = new HashMap<>();

    static {
        fieldHandlers.put((byte) 0xFE, RedisParser::handleDatabaseSelector);
        fieldHandlers.put((byte) 0xFD, RedisParser::handleKeyValuePairWithExpirySeconds);
        fieldHandlers.put((byte) 0xFC, RedisParser::handleKeyValuePairWithExpiryMilliseconds);
    }

    public static Map<String, Map<String, ? extends Object>> parseRedisFile(String filePath) {
        Map<String, String> hashMap = new HashMap<>();
        Map<String, Double> expiryTimes = new HashMap<>();

        try (DataInputStream file = new DataInputStream(new FileInputStream(filePath))) {
            byte[] magicString = new byte[5];
            file.readFully(magicString);
            byte[] rdbVersion = new byte[4];
            file.readFully(rdbVersion);

            while (true) {
                byte b = file.readByte();
                if (b == (byte) 0xFE) {
                    for (int i = 0; i < 4; i++) {
                        System.out.println(file.readByte());
                    }
                    break;
                }
            }

            while (true) {
                byte fieldType = file.readByte();
                if (fieldType == (byte) 0xFF) {
                    break;
                }

                Object[] result;
                if (fieldHandlers.containsKey(fieldType)) {
                    result = fieldHandlers.get(fieldType).handle(file);
                } else {
                    result = handleKeyValuePairWithoutExpiry(file, fieldType);
                }

                String key = (String) result[0];
                Object value = result[1];
                double expiryTime = (double) result[2];

                if (key != null && value != null) {
                    hashMap.put(key, (String) value);
                }
                if (key != null) {
                    expiryTimes.put(key, expiryTime);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + filePath);
        } catch (IOException e) {
            System.out.println("Error occurred while parsing the file: " + e.getMessage());
        }

        return Map.of("hashMap", hashMap, "expiryTimes", expiryTimes);
    }

}
