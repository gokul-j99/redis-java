package utils;

import java.util.*;
import java.nio.charset.StandardCharsets;

import static utils.Constants.*;


public class EncodingUtils {

   // public static final String EMPTY_ARRAY_RESPONSE = "+(empty array)\r\n";
   // public static final String NOT_FOUND_RESPONSE = "$-1\r\n";
  //  public static final String WRONG_TYPE_RESPONSE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    public static String createRedisResponse(Object response) {
        if (response == null) {
            return "$-1\r\n";
        } else if (response instanceof String) {
            String strResponse = (String) response;
            if (strResponse.equals("OK")) {
                return "+" + strResponse + "\r\n";
            } else if (strResponse.startsWith("-ERR") || strResponse.startsWith("+ERR")) {
                return strResponse + "\r\n";
            }
            return "$" + strResponse.length() + "\r\n" + strResponse + "\r\n";
        } else if (response instanceof Integer) {
            return ":" + response + "\r\n";
        } else if (response instanceof List) {
            return generateRedisArray((List<String>) response);
        }
        return null;
    }


    private static int findIndex(byte[] data, byte value) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] == value) {
                return i;
            }
        }
        return -1;
    }

    public static boolean isError(String data) {
        return data.startsWith("-ERR") || data.startsWith("+ERR");
    }

    public static String generateRedisArray(List<String> lst) {
        StringBuilder redisArray = new StringBuilder();
        if (lst.isEmpty()) {
            return "*0\r\n";
        }
        redisArray.append("*").append(lst.size()).append("\r\n");
        for (String element : lst) {
            if (isBulkString(element) || isError(element)) {
                redisArray.append(element);
                continue;
            }
            if (element.endsWith("\r\n")) {
                element = element.substring(0, element.length() - 2);
            }

            if (element.startsWith(":") && element.substring(1).matches("\\d+")) {
                redisArray.append(element).append("\r\n");
                continue;
            }
            redisArray.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
        }
        return redisArray.toString();
    }

    private static boolean isBulkString(String data) {
        // should start with $, end with \r\n. The digits following $ should reflect the length of the string
        if (!data.startsWith("$") || !data.endsWith("\r\n")) {
            return false;
        }
        int index = 1;
        while (index < data.length() && Character.isDigit(data.charAt(index))) {
            index++;
        }
        int strLen = Integer.parseInt(data.substring(1, index));
        index += 2; // Skip \r\n
        return data.substring(index).length() == strLen;
    }
    public static String asBulkString(String payload) {
        return "$" + payload.length() + "\r\n" + payload + "\r\n";
    }
    public static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random rnd = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }
        return sb.toString();
    }

    public static String getValue(AsyncRequestHandler handler, String key) {
        Long expiration = handler.expiration.get(key);
        if (expiration != null && expiration < System.currentTimeMillis() / 1000) {
            handler.memory.remove(key);
            handler.expiration.remove(key);
            return null;
        } else {
            Object value = handler.memory.get(key);
            if (value == null) {
                return NOT_FOUND_RESPONSE;
            } else if (!(value instanceof String)) {
                return WRONG_TYPE_RESPONSE;
            } else {
                return (String) value;
            }
        }
    }
    public static List<List<String>> parseRedisProtocol(byte[] data) {
        List<List<String>> commands = new ArrayList<>();
        int index = 0;

        while (index < data.length) {
            if (data[index] == '*') {
                int numElementsEnd = index + 1;
                while (numElementsEnd < data.length && data[numElementsEnd] != '\r') {
                    numElementsEnd++;
                }

                if (numElementsEnd >= data.length || data[numElementsEnd + 1] != '\n') {
                    break;
                }

                int numElements = Integer.parseInt(new String(data, index + 1, numElementsEnd - index - 1, StandardCharsets.UTF_8));
                index = numElementsEnd + 2;

                List<String> command = new ArrayList<>();
                for (int i = 0; i < numElements; i++) {
                    if (data[index] != '$') {
                        break;
                    }

                    int elementLengthEnd = index + 1;
                    while (elementLengthEnd < data.length && data[elementLengthEnd] != '\r') {
                        elementLengthEnd++;
                    }

                    if (elementLengthEnd >= data.length || data[elementLengthEnd + 1] != '\n') {
                        break;
                    }

                    int elementLength = Integer.parseInt(new String(data, index + 1, elementLengthEnd - index - 1, StandardCharsets.UTF_8));
                    index = elementLengthEnd + 2;

                    String element = new String(data, index, elementLength, StandardCharsets.UTF_8);
                    command.add(element);
                    index += elementLength + 2;
                }
                commands.add(command);
            } else {
                index++;
            }
        }

        return commands;
    }
    public static List<Integer> getCommandLengths(byte[] data) {
        List<Integer> commandLengths = new ArrayList<>();
        int index = 0;

        while (index < data.length) {
            if (data[index] == '*') {
                int numElementsEnd = index + 1;
                while (numElementsEnd < data.length && data[numElementsEnd] != '\r') {
                    numElementsEnd++;
                }

                if (numElementsEnd >= data.length || data[numElementsEnd + 1] != '\n') {
                    break;
                }

                int numElements = Integer.parseInt(new String(data, index + 1, numElementsEnd - index - 1, StandardCharsets.UTF_8));
                index = numElementsEnd + 2;

                int commandLength = 0;
                for (int i = 0; i < numElements; i++) {
                    if (data[index] != '$') {
                        break;
                    }

                    int elementLengthEnd = index + 1;
                    while (elementLengthEnd < data.length && data[elementLengthEnd] != '\r') {
                        elementLengthEnd++;
                    }

                    if (elementLengthEnd >= data.length || data[elementLengthEnd + 1] != '\n') {
                        break;
                    }

                    int elementLength = Integer.parseInt(new String(data, index + 1, elementLengthEnd - index - 1, StandardCharsets.UTF_8));
                    index = elementLengthEnd + 2 + elementLength + 2;
                    commandLength += elementLength + 1; // Adding 1 for the space between elements
                }

                commandLengths.add(commandLength);
            } else {
                index++;
            }
        }

        return commandLengths;
    }
    public static byte[] encodeRedisProtocol(List<String> data) {
        StringBuilder encodedData = new StringBuilder();
        encodedData.append("*").append(data.size()).append("\r\n");

        for (String element : data) {
            encodedData.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
        }

        return encodedData.toString().getBytes();
    }


        }
