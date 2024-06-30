import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class ClientHandler implements Runnable {
    private Socket clientSocket;
    private static ConcurrentHashMap<String, String> setDict = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Long> expiryDict = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    public static String role = "master"; // Default role

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("*")) {
                    int numArgs = Integer.parseInt(line.substring(1).trim());
                    String[] args = new String[numArgs];
                    for (int i = 0; i < numArgs; i++) {
                        reader.readLine(); // Read and ignore the length line
                        args[i] = reader.readLine(); // Read the actual argument
                    }
                    String command = args[0].toUpperCase();

                    if (command.equals("SET")) {
                        String key = args[1];
                        String value = args[2];
                        setDict.put(key, value);
                        clientSocket.getOutputStream().write("+OK\r\n".getBytes());

                        if (numArgs > 3 && args[3].equalsIgnoreCase("PX")) {
                            long expiryTime = Long.parseLong(args[4]);
                            long expiryTimestamp = System.currentTimeMillis() + expiryTime;
                            expiryDict.put(key, expiryTimestamp);

                            scheduler.schedule(() -> {
                                setDict.remove(key);
                                expiryDict.remove(key);
                            }, expiryTime, TimeUnit.MILLISECONDS);
                        }
                    } else if (command.equals("GET")) {
                        String key = args[1];
                        if (expiryDict.containsKey(key) && expiryDict.get(key) < System.currentTimeMillis()) {
                            setDict.remove(key);
                            expiryDict.remove(key);
                        }
                        String value = setDict.get(key);
                        if (value != null) {
                            clientSocket.getOutputStream().write(
                                    String.format("$%d\r\n%s\r\n", value.length(), value).getBytes());
                        } else {
                            clientSocket.getOutputStream().write("$-1\r\n".getBytes());
                        }
                    } else if (command.equals("ECHO")) {
                        String message = args[1];
                        clientSocket.getOutputStream().write(
                                String.format("$%d\r\n%s\r\n", message.length(), message).getBytes());
                    } else if (command.equals("PING")) {
                        clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                    } else if (command.equals("INFO") && numArgs > 1 && args[1].equalsIgnoreCase("replication")) {
                        String response = "role:" + role + "\r\n";
                        clientSocket.getOutputStream().write(
                                String.format("$%d\r\n%s", response.length(), response).getBytes());
                    }
                }

                if (line.isEmpty()) {
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("IOException while handling client: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException while closing client socket: " + e.getMessage());
            }
        }
    }
}

