import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");


        int port = 6379;
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            System.out.println("Server started and listening on port " + port);

            // Event loop to handle multiple clients
            while (true) {
                try {
                    // Wait for connection from client
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("Accepted connection from client");

                    // Handle the client in a separate thread
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

