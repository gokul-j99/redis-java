import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;
        String role = "master";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            } else if (args[i].equalsIgnoreCase("--replicaof") && i + 2 < args.length) {
                role = "slave";
            }
        }

        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress("localhost", port));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("***Server started on port " + port + "***");
        ClientHandler.setRole(role);

        while (true) {
            int conns = selector.select();
            if (conns == 0) continue;

            Set<SelectionKey> selectedKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectedKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey key = iterator.next();

                if (key.isAcceptable()) {
                    ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
                    SocketChannel clientChannel = serverChannel.accept();
                    clientChannel.configureBlocking(false);
                    clientChannel.register(selector, SelectionKey.OP_READ);
                    System.out.println("Connection accepted: " + clientChannel.getRemoteAddress());

                } else if (key.isReadable()) {
                    SocketChannel clientChannel = (SocketChannel) key.channel();
                    ByteBuffer buffer = ByteBuffer.allocate(256);
                    int bytesRead = clientChannel.read(buffer);

                    if (bytesRead == -1) {
                        clientChannel.close();
                    } else {
                        buffer.flip();
                        String message = new String(buffer.array(), 0, bytesRead).trim();
                        System.out.println("[Received message] " + message);

                        String response = ClientHandler.processRequest(message);
                        System.out.println("[Response] " + response);

                        buffer.clear();
                        buffer.put(response.getBytes());
                        buffer.flip();
                        clientChannel.write(buffer);
                    }
                }
                iterator.remove();
            }
        }
    }
}
