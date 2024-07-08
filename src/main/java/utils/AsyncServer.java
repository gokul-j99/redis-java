package utils;

import commands.*;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class AsyncServer {
    private final String host;
    private final int port;
    private final String replicaServer;
    private final int replicaPort;
    private final Map<String, String> memory;
    private final Map<String, Long> expiration;
    public final Map<String, Map<Integer, Map<Integer, List<String>>>> streamstore;
    public final List<BufferedWriter> writers;
    public int numacks;
    public final Map<String, String> config;
    public final Map<String, RedisCommand> commandMap;

    private ServerSocket serverSocket;
    private ExecutorService executor;

    public AsyncServer(String host, int port, String replicaServer, int replicaPort, String dir, String dbfilename) throws IOException {
        this.host = host;
        this.port = port;
        this.replicaServer = replicaServer;
        this.replicaPort = replicaPort;
        this.memory = new HashMap<>();
        this.expiration = new HashMap<>();
        this.streamstore = new HashMap<>();
        this.writers = new ArrayList<>();
        this.numacks = 0;
        this.config = new HashMap<>();
        this.config.put("dir", dir);
        this.config.put("dbfilename", dbfilename);
        this.commandMap = initializeCommandMap();

        if (!dir.isEmpty() && !dbfilename.isEmpty()) {
            Path filePath = Paths.get(dir, dbfilename);
            parseRedisFile(filePath);
        }
    }

    private Map<String, RedisCommand> initializeCommandMap() {
        Map<String, RedisCommand> map = new HashMap<>();
        map.put("PING", new PingCommand());
        map.put("ECHO", new EchoCommand());
        map.put("SET", new SetCommand());
        map.put("GET", new GetCommand());
        map.put("INFO", new InfoCommand());
        map.put("REPLCONF", new ReplConfCommand());
        map.put("PSYNC", new PSyncCommand());
        map.put("WAIT", new WaitCommand());
        map.put("CONFIG", new ConfigCommand());
        map.put("KEYS", new KeysCommand());
        map.put("TYPE", new TypeCommand());
        map.put("XADD", new XAddCommand());
        map.put("XRANGE", new XRangeCommand());
        map.put("XREAD", new XReadCommand());
        map.put("INCR", new IncrCommand());
        map.put("INCRBY", new IncrByCommand());
        map.put("EXEC", new ExecCommand());
        map.put("MULTI", new MultiCommand());
        map.put("DISCARD", new DiscardCommand());
        return map;
    }

    public static AsyncServer create(String host, int port, String replicaServer, int replicaPort, String dir, String dbfilename) throws IOException {
        AsyncServer instance = new AsyncServer(host, port, replicaServer, replicaPort, dir, dbfilename);
        instance.start();
        if (replicaServer != null && replicaPort > 0) {
            instance.connectToReplicaServer(replicaServer, replicaPort);
        }
        return instance;
    }

    private void start() throws IOException {
        serverSocket = new ServerSocket(port);
        executor = Executors.newCachedThreadPool();
        Logger.getLogger(AsyncServer.class.getName()).info("Server started at http://" + host + ":" + port);
        executor.submit(() -> {
            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    executor.submit(new AsyncRequestHandler(clientSocket, this));
                } catch (IOException e) {
                    Logger.getLogger(AsyncServer.class.getName()).log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        });
    }

    private void connectToReplicaServer(String replicaServer, int replicaPort) throws IOException {
        try (Socket socket = new Socket(replicaServer, replicaPort);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {

            String response = sendPing(reader, writer);
            if (!response.equals("+PONG")) {
                throw new IOException("Failed to receive PONG from replica server. Received: " + response);
            }

            sendReplconfCommand(reader, writer, port);
            sendAdditionalReplconfCommand(reader, writer);
            sendPsyncCommand(reader, writer);

            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                System.out.println("Received input: " + line);
                if (line.startsWith("$")) {
                    int length = Integer.parseInt(line.substring(1));
                    char[] buffer = new char[length];
                    int bytesRead = reader.read(buffer, 0, length);
                    if (bytesRead < length) {
                        System.out.println("Reached end of RDB file unexpectedly");
                        break;
                    }
                    String rdbData = new String(buffer);
                    processRDBData(rdbData);
                } else {
                    processMasterCommand(line, reader);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processMasterCommand(String commandLine, BufferedReader reader) throws Exception {
        System.out.println("Received command from master: " + commandLine);
        try {
            List<List<String>> commandList = EncodingUtils.parseRedisProtocol(commandLine.getBytes(StandardCharsets.UTF_8));
            for (List<String> command : commandList) {
                System.out.println("Processing command from master: " + command);
                RedisCommand commandClass = commandMap.getOrDefault(command.get(0).toUpperCase(), new UnknownCommand());
                commandClass.execute(new AsyncRequestHandler(null, this), command);
            }
        } catch (Exception e) {
            System.err.println("Error processing command from master: " + commandLine);
            e.printStackTrace();
        }
    }



    private void processRDBData(String rdbData) throws IOException {
        // Create a temporary file to store the RDB data
        Path tempFile = Files.createTempFile("redis", ".rdb");
        System.out.println(rdbData);
        try (BufferedWriter writer = Files.newBufferedWriter(tempFile, StandardCharsets.UTF_8)) {
            writer.write(rdbData);
        }
        parseRedisFile(tempFile);
        Files.delete(tempFile);
    }



    private void sendReplconfCommand(BufferedReader reader, BufferedWriter writer, int port) throws IOException {
        String replconfCommand = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n" + port + "\r\n";
        writer.write(replconfCommand);
        writer.flush();
        String replconfResponse = reader.readLine();
        if (!"+OK".equals(replconfResponse)) {
            throw new IOException("Failed to receive +OK response from REPLCONF command. Received: " + replconfResponse);
        }
    }

    private void sendAdditionalReplconfCommand(BufferedReader reader, BufferedWriter writer) throws IOException {
        String replconfCommandAdditional = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        writer.write(replconfCommandAdditional);
        writer.flush();
        String replconfResponseAdditional = reader.readLine();
        if (!"+OK".equals(replconfResponseAdditional)) {
            throw new IOException("Failed to receive +OK response from additional REPLCONF command. Received: " + replconfResponseAdditional);
        }
    }

    private void sendPsyncCommand(BufferedReader reader, BufferedWriter writer) throws IOException {
        String psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        writer.write(psyncCommand);
        writer.flush();
        // Read the PSYNC response if needed
    }

    private String sendPing(BufferedReader reader, BufferedWriter writer) throws IOException {
        writer.write("*1\r\n$4\r\nPING\r\n");
        writer.flush();
        return reader.readLine();
    }

    private void handleReplicaCommunication(BufferedReader reader, BufferedWriter writer) throws IOException {
        while (true) {
            String response = reader.readLine();
            if (response == null) {
                break;
            }
            // Handle response if needed
        }
    }

    public List<String> getKeysArray() {
        try {
            Path filePath = Paths.get(config.get("dir"), config.get("dbfilename"));
            Map<String, String> hashMap = parseRedisFile(filePath);

            return new ArrayList<>(hashMap.keySet());
        } catch (IOException e) {
            System.out.println("Error reading keys from file");
            Logger.getLogger(AsyncServer.class.getName()).log(Level.SEVERE, "Error reading keys from file", e);
            return Collections.emptyList();
        }
    }

    public String asArray(Collection<String> data) {
        StringBuilder encodedData = new StringBuilder();
        encodedData.append("*").append(data.size()).append("\r\n");
        for (String element : data) {
            encodedData.append("$").append(element.length()).append("\r\n").append(element).append("\r\n");
        }
        return encodedData.toString();
    }
    private Map<String, String> parseRedisFile(Path filePath) throws IOException {
        Map<String, String> memory = new HashMap<>();
        Map<String, Long> expiration = new HashMap<>();

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(Files.newInputStream(filePath)))) {
            // Read the magic string and version
            byte[] magic = new byte[5];
            dis.readFully(magic);
            String magicString = new String(magic);
            if (!"REDIS".equals(magicString)) {
                throw new IOException("Invalid RDB file format");
            }

            // Read the version
            byte[] versionBytes = new byte[4];
            dis.readFully(versionBytes);
            int version = Integer.parseInt(new String(versionBytes));
            if (version < 1 || version > 11) { // Updated to support version up to 11
                throw new IOException("Unsupported RDB file version: " + version);
            }

            while (true) {
                int type;
                try {
                    type = dis.readByte();
                } catch (EOFException e) {
                    break; // End of file reached
                }

                if (type == (byte) 0xFF) { // End of file marker
                    break;
                }

                long expireTime = -1;
                if (type == (byte) 0xFD) { // Millisecond precision expire time
                    expireTime = dis.readLong();
                    type = dis.readByte(); // Read the actual type of the next object
                } else if (type == (byte) 0xFC) { // Second precision expire time
                    expireTime = dis.readInt() * 1000L;
                    type = dis.readByte(); // Read the actual type of the next object
                }

                switch (type) {
                    case (byte) 0x00: // String key-value pair
                        String key = readString(dis);
                        String value = readString(dis);
                        memory.put(key, value);
                        if (expireTime > 0) {
                            expiration.put(key, expireTime);
                        }
                        break;
                    case (byte) 0x01: // List of strings
                        // Implement parsing for list of strings if needed
                        skipObject(dis);
                        break;
                    case (byte) 0x02: // Set of strings
                        // Implement parsing for set of strings if needed
                        skipObject(dis);
                        break;
                    case (byte) 0x03: // Sorted set
                        // Implement parsing for sorted set if needed
                        skipObject(dis);
                        break;
                    case (byte) 0x04: // Hash table
                        // Implement parsing for hash table if needed
                        skipObject(dis);
                        break;
                    default: // Unsupported or unknown type
                        skipObject(dis);
                        break;
                }
            }
        } catch (EOFException e) {
            // Handle end of file gracefully
            System.err.println("Reached end of RDB file unexpectedly");
        }
        this.memory.putAll(memory);
        this.expiration.putAll(expiration);
        return memory;
    }

    private void skipObject(DataInputStream dis) throws IOException {
        // Skip length-prefixed object data
        int length = readLength(dis);
        if (length >= 0) {
            dis.skipBytes(length);
        } else {
            throw new IOException("Invalid length encoding in RDB file");
        }
    }

    private String readString(DataInputStream dis) throws IOException {
        int length = readLength(dis);
        byte[] bytes = new byte[length];
        dis.readFully(bytes);
        return new String(bytes);
    }

    private int readLength(DataInputStream dis) throws IOException {
        int length = dis.readByte() & 0xFF;
        if (length >= 0xC0) {
            switch (length) {
                case 0xC0:
                case 0xC1:
                    throw new IOException("Invalid length encoding in RDB file");
                case 0xC2:
                    length = ((length & 0x3F) << 8) | (dis.readByte() & 0xFF);
                    break;
                case 0xC3:
                    length = dis.readInt();
                    break;
                default:
                    throw new IOException("Unsupported length encoding in RDB file");
            }
        }
        return length;
    }


    public Map<String, String> getMemory() {
        return memory;
    }

    public Map<String, Long> getExpiration() {
        return expiration;
    }

    public String getReplicaServer() {
        return replicaServer;
    }

    public int getReplicaPort() {
        return replicaPort;
    }

    public List<BufferedWriter> getWriters() {
        return writers;
    }

    public void incrementNumAcks() {
        numacks++;
    }

    public Map<String, Map<Integer, Map<Integer, List<String>>>> getStreamstore() {
        return streamstore;
    }
}
