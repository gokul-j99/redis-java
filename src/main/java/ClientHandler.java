import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

class ClientHandler implements Runnable {
    private Socket clientSocket;
    private static ConcurrentHashMap<String, String> setDict =
            new ConcurrentHashMap<>();

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.toLowerCase().contains("ping")) {
                    clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
                }
                else if (line.equalsIgnoreCase("ECHO")) {
                    reader.readLine();
                    String message = reader.readLine();
                    clientSocket.getOutputStream().write(
                            String.format("$%d\r\n%s\r\n", message.length(), message)
                                    .getBytes());
                }
                else if (line.equalsIgnoreCase("SET")) {

                    String str = reader.readLine();
                    System.out.println(str);
                    setDict.put("","");
                    clientSocket.getOutputStream().write("+OK\r\n".getBytes());
                }
                else if (line.equalsIgnoreCase("GET")) {


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