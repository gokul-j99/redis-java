import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static SocketChannel masterChannel = null;

    public static void main(String[] args) throws IOException {
        System.out.println("Logs from your program will appear here!");

        int port = 6379;
        String role = "master";
        String masterHost = null;
        int masterPort = 0;

        for (int i = 0; i < args.length; i++) {
            System.out.println("*** " + args[i] + " ***");
            if (args[i].equalsIgnoreCase("--port") && i + 1 < args.length) {
                port = Integer.parseInt(args[i + 1]);
            } else if (args[i].equalsIgnoreCase("--replicaof") && i + 2 < args.length) {
                System.out.println("*** role " + args[i] + " ***");
                role = "slave";
                masterHost = args[i + 1];
                masterPort = Integer.parseInt(args[i + 2]);
            }
        }

        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress("localhost", port));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("***Server started on port " + port + "***");
        ClientHandler.setRole(role);

        // If role is slave, connect to master
        if ("slave".equals(role)) {
            connectToMaster(masterHost, masterPort, selector);
        }

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
                } else if (key.isConnectable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    if (channel.finishConnect()) {
                        key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                        System.out.println("Connected to master: " + channel.getRemoteAddress());
                    }
                } else if (key.isWritable()) {
                    SocketChannel channel = (SocketChannel) key.channel();
                    if (channel == masterChannel) {
                        // Send PING to master
                        String pingCommand = "*1\r\n$4\r\nPING\r\n";
                        ByteBuffer buffer = ByteBuffer.wrap(pingCommand.getBytes(StandardCharsets.UTF_8));
                        channel.write(buffer);
                        System.out.println("Sent PING to master");
                        key.interestOps(SelectionKey.OP_READ); // After sending PING, only interested in reading responses
                    }
                }
                iterator.remove();
            }
        }
    }

    private static void connectToMaster(String masterHost, int masterPort, Selector selector) {
        try {
            masterChannel = SocketChannel.open();
            masterChannel.configureBlocking(false);
            masterChannel.connect(new InetSocketAddress(masterHost, masterPort));
            masterChannel.register(selector, SelectionKey.OP_CONNECT);
            System.out.println("Connecting to master at " + masterHost + ":" + masterPort);
        } catch (IOException e) {
            System.out.println("IOException when connecting to master: " + e.getMessage());
        }
    }
}

class ClientHandler implements Runnable {
    private Socket clientSocket;
    private static ConcurrentHashMap<String, String> setDict = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Long> expiryDict = new ConcurrentHashMap<>();
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private static String role = "master";
    private static final String MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private static final long MASTER_REPL_OFFSET = 0;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public static void setRole(String newRole) {
        role = newRole;
    }

    public static String processRequest(String message) {
        String[] lines = message.split("\r\n");
        String command = lines[2].toUpperCase();
        StringBuilder response = new StringBuilder();

        switch (command) {
            case "SET":
                String key = lines[4];
                String value = lines[6];
                setDict.put(key, value);
                response.append("+OK\r\n");

                if (lines.length > 8 && lines[8].equalsIgnoreCase("PX")) {
                    long expiryTime = Long.parseLong(lines[10]);
                    long expiryTimestamp = System.currentTimeMillis() + expiryTime;
                    expiryDict.put(key, expiryTimestamp);

                    scheduler.schedule(() -> {
                        setDict.remove(key);
                        expiryDict.remove(key);
                    }, expiryTime, TimeUnit.MILLISECONDS);
                }
                break;

            case "GET":
                key = lines[4];
                if (expiryDict.containsKey(key) && expiryDict.get(key) < System.currentTimeMillis()) {
                    setDict.remove(key);
                    expiryDict.remove(key);
                }
                value = setDict.get(key);
                if (value != null) {
                    response.append(String.format("$%d\r\n%s\r\n", value.length(), value));
                } else {
                    response.append("$-1\r\n");
                }
                break;

            case "ECHO":
                String messageContent = lines[4];
                response.append(String.format("$%d\r\n%s\r\n", messageContent.length(), messageContent));
                break;

            case "PING":
                response.append("+PONG\r\n");
                break;

            case "INFO":
                if (lines.length > 4 && lines[4].equalsIgnoreCase("replication")) {
                    String infoResponse = String.format("role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n",
                            role, MASTER_REPLID, MASTER_REPL_OFFSET);
                    response.append(String.format("$%d\r\n%s\r\n", infoResponse.length(), infoResponse));
                }
                break;

            default:
                response.append("-ERR unknown command\r\n");
                break;
        }
        return response.toString();
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String response = processRequest(line);
                OutputStream out = clientSocket.getOutputStream();
                out.write(response.getBytes(StandardCharsets.UTF_8));
                out.flush();
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
