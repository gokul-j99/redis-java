import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

class ClientHandler implements Runnable {
    private final Socket clientSocket;
    private static ConcurrentHashMap<String, String> setDict = new ConcurrentHashMap<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(" ");
                String command = tokens[0].toUpperCase();

                if (command.equals("PING")) {
                    clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                } else if (command.equals("ECHO")) {
                    String message = reader.readLine();
                    clientSocket.getOutputStream().write(
                            String.format("$%d\r\n%s\r\n", message.length(), message)
                                    .getBytes());
                } else if (command.equals("SET")) {
                    String key = tokens[1];
                    String value = tokens[2];
                    setDict.put(key, value);
                    clientSocket.getOutputStream().write("+OK\r\n".getBytes());
                } else if (command.equals("GET")) {
                    String key = tokens[1];
                    String value = setDict.get(key);
                    if (value != null) {
                        clientSocket.getOutputStream().write(
                                String.format("$%d\r\n%s\r\n", value.length(), value).getBytes());
                    } else {
                        clientSocket.getOutputStream().write("$-1\r\n".getBytes());
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
