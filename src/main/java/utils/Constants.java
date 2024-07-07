package utils;

public class Constants {
    public static final String NOT_FOUND_RESPONSE = "$-1\r\n";
    public static final String NIL_RESPONSE = "+nil\r\n";
    public static final String SYNTAX_ERROR = "-ERR syntax error\r\n";
    public static final String WRONG_TYPE_RESPONSE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    public static final String NON_INT_ERROR = "-ERR value is not an integer or out of range\r\n";
    public static final String EMPTY_ARRAY_RESPONSE = "+(empty array)\r\n";
    public static final String OUT_OF_RANGE_RESPONSE = "+ERR index out of range";
    public static final String FLOAT_ERROR_MESSAGE = "-ERR value is not a valid float\r\n";
    public static final String EXEC_WITHOUT_MULTI = "-ERR EXEC without MULTI\r\n";
    public static final String DISCARD_WITHOUT_MULTI = "-ERR DISCARD without MULTI\r\n";
}

