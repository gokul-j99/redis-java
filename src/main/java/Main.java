import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        System.out.println("Logs from your program will appear here!");

        int port = 6379; // Default port
        String masterHost = null;
        int masterPort = 0;

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            } else if (args[i].equals("--replicaof") && i + 2 < args.length) {
                masterHost = args[i + 1];
                masterPort = Integer.parseInt(args[i + 2]);
                ClientHandler.role = "slave"; // Set role to slave
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