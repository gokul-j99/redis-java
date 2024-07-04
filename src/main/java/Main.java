import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.*;

public class Main {
    private static final int DEFAULT_PORT = 6379;
    private static final RedisServerRole DEFAULT_ROLE = RedisServerRole.MASTER;
    private static SocketChannel masterChannel = null;

    public static void main(String[] args) throws IOException, ParseException {
        CommandLine commandLine = parsePortFromCommandLineArgs(args);
        int portNumber = DEFAULT_PORT;
        if (commandLine.hasOption("port")) {
            portNumber = Integer.parseInt(commandLine.getOptionValue("port"));
        }
        System.out.println("Port number is: " + portNumber);
        RedisServerRole role = DEFAULT_ROLE;
        if (commandLine.hasOption("replicaof")) {
            String[] replicaArgsArray = commandLine.getOptionValues("replicaof");
            if (replicaArgsArray.length != 2) {
                throw new MissingArgumentException("Missing argument for option: replicaof");
            }
            String hostname = replicaArgsArray[0];
            int masterPortNumber = Integer.parseInt(replicaArgsArray[1]);
            System.out.println("Replicating from master, master host is: " +
                    hostname + ", port number is: " + masterPortNumber);
            role = RedisServerRole.SLAVE;
            connectToMaster(hostname, masterPortNumber);
        }
        RedisDatastore datastore = new RedisDatastore();
        Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress("localhost", portNumber));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        System.out.println("***Server started on port " + portNumber + "***");
        ClientHandler.setRole(role.toString());
        ClientHandler.setDatastore(datastore);

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

    private static void connectToMaster(String hostname, int masterPortNumber) throws IOException {
        try {
            masterChannel = SocketChannel.open();
            masterChannel.configureBlocking(false);
            masterChannel.connect(new InetSocketAddress(hostname, masterPortNumber));
            System.out.println("Connecting to master at " + hostname + ":" + masterPortNumber);
        } catch (IOException e) {
            System.out.println("IOException when connecting to master: " + e.getMessage());
        }
    }

    private static CommandLine parsePortFromCommandLineArgs(String[] args) throws ParseException {
        CommandLine commandLine;
        Options options = new Options();
        Option portOption = Option.builder()
                .required(false)
                .desc("port to run redis on")
                .longOpt("port")
                .hasArg(true)
                .build();
        options.addOption(portOption);
        Option replicaOption = Option.builder()
                .required(false)
                .desc("Master port to replicate from")
                .longOpt("replicaof")
                .hasArg(true)
                .numberOfArgs(2)
                .build();
        options.addOption(replicaOption);
        CommandLineParser parser = new DefaultParser();
        commandLine = parser.parse(options, args);
        return commandLine;
    }
}

class ClientHandler implements Runnable {
    private Socket clientSocket;
    private static RedisDatastore datastore;
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

    public static void setDatastore(RedisDatastore newDatastore) {
        datastore = newDatastore;
    }

    public static String processRequest(String message) {
        String[] lines = message.split("\r\n");
        String command = lines[2].toUpperCase();
        StringBuilder response = new StringBuilder();

        switch (command) {
            case "SET":
                String key = lines[4];
                String value = lines[6];
                datastore.set(key, value);
                response.append("+OK\r\n");

                if (lines.length > 8 && lines[8].equalsIgnoreCase("PX")) {
                    long expiryTime = Long.parseLong(lines[10]);
                    datastore.set(key, value, expiryTime);
                }
                break;

            case "GET":
                key = lines[4];
                Optional<String> valueOpt = datastore.get(key);
                if (valueOpt.isPresent()) {
                    value = valueOpt.get();
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

enum RedisServerRole {
    MASTER,
    SLAVE
}


class RedisDatastore {
    private ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> expiry = new ConcurrentHashMap<>();

    public void set(String key, String value) {
        store.put(key, value);
        expiry.remove(key);
    }

    public void set(String key, String value, long milliseconds) {
        store.put(key, value);
        long expiryTime = System.currentTimeMillis() + milliseconds;
        expiry.put(key, expiryTime);
    }

    public Optional<String> get(String key) {
        if (expiry.containsKey(key) && System.currentTimeMillis() > expiry.get(key)) {
            store.remove(key);
            expiry.remove(key);
            return Optional.empty();
        }
        return Optional.ofNullable(store.get(key));
    }
}

