//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//
//public class ClientHandler {
//    private static ConcurrentHashMap<String, String> setDict = new ConcurrentHashMap<>();
//    private static ConcurrentHashMap<String, Long> expiryDict = new ConcurrentHashMap<>();
//    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
//    private static String role = "master";
//
//    public static void setRole(String newRole) {
//        role = newRole;
//    }
//
//    public static String processRequest(String message) {
//        String[] lines = message.split("\r\n");
//        String command = lines[2].toUpperCase();
//        StringBuilder response = new StringBuilder();
//
//        switch (command) {
//            case "SET":
//                String key = lines[4];
//                String value = lines[6];
//                setDict.put(key, value);
//                response.append("+OK\r\n");
//
//                if (lines.length > 8 && lines[8].equalsIgnoreCase("PX")) {
//                    long expiryTime = Long.parseLong(lines[10]);
//                    long expiryTimestamp = System.currentTimeMillis() + expiryTime;
//                    expiryDict.put(key, expiryTimestamp);
//
//                    scheduler.schedule(() -> {
//                        setDict.remove(key);
//                        expiryDict.remove(key);
//                    }, expiryTime, TimeUnit.MILLISECONDS);
//                }
//                break;
//
//            case "GET":
//                key = lines[4];
//                if (expiryDict.containsKey(key) && expiryDict.get(key) < System.currentTimeMillis()) {
//                    setDict.remove(key);
//                    expiryDict.remove(key);
//                }
//                value = setDict.get(key);
//                if (value != null) {
//                    response.append(String.format("$%d\r\n%s\r\n", value.length(), value));
//                } else {
//                    response.append("$-1\r\n");
//                }
//                break;
//
//            case "ECHO":
//                String messageContent = lines[4];
//                response.append(String.format("$%d\r\n%s\r\n", messageContent.length(), messageContent));
//                break;
//
//            case "PING":
//                response.append("+PONG\r\n");
//                break;
//
//            case "INFO":
//                if (lines.length > 4 && lines[4].equalsIgnoreCase("replication")) {
//                    String infoResponse = "role:" + role + "\r\n";
//                    response.append(String.format("$%d\r\n%s", infoResponse.length(), infoResponse));
//                }
//                break;
//
//            default:
//                response.append("-ERR unknown command\r\n");
//                break;
//        }
//        return response.toString();
//    }
//}
