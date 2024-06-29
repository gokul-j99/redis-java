import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379; // Default port
        if (args.length > 1 && args[0].equals("--port")) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.out.println("Invalid port number, using default port 6379");
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.out.println("Server started and listening on port " + port);

            // Event loop to handle multiple clients
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Accepted connection from client");

                    new Thread(new ClientHandler(clientSocket)).start();
                } catch (IOException e) {
                    System.out.println("IOException when accepting connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.out.println("IOException when setting up server: " + e.getMessage());
        }
    }
}
