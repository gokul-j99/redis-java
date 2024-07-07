package utils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import java.util.zip.GZIPInputStream;

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

        Logger.getLogger(AsyncServer.class.getName()).info("Master server running on port: " + port);
        if (!dir.isEmpty() && !dbfilename.isEmpty()) {
            Path filePath = Paths.get(dir, dbfilename);
            parseRedisFile(filePath);
        }
    }

    public static AsyncServer create(String host, int port, String replicaServer, int replicaPort, String dir, String dbfilename) throws IOException {
        AsyncServer instance = new AsyncServer(host, port, replicaServer, replicaPort, dir, dbfilename);
        instance.start();
        if (replicaServer != null && replicaPort > 0) {
            Logger.getLogger(AsyncServer.class.getName()).info("Connecting to master at " + replicaServer + ":" + replicaPort);
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
            if (!"+PONG\r\n".equals(response)) {
                throw new IOException("Failed to receive PONG from replica server. Received: " + response);
            }

            sendReplconfCommand(reader, writer, port);
            sendAdditionalReplconfCommand(reader, writer);
            sendPsyncCommand(reader, writer);
            handleReplicaCommunication(reader, writer);
        }
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
        Map<String, String> hashMap = new HashMap<>();
        Map<String, Long> expirationMap = new HashMap<>();
        try (DataInputStream dis = new DataInputStream(new GZIPInputStream(new FileInputStream(filePath.toFile())))) {
            byte[] buffer = new byte[5];
            dis.readFully(buffer); // Read "REDIS" magic string
            dis.readInt(); // Read RDB version

            while (dis.available() > 0) {
                int dataType = dis.readUnsignedByte();
                switch (dataType) {
                    case 0x00: // String
                        String key = readString(dis);
                        String value = readString(dis);
                        hashMap.put(key, value);
                        break;
                    case 0xFD: // Expired key (seconds)
                        long expireTimeSeconds = dis.readInt();
                        dataType = dis.readUnsignedByte();
                        key = readString(dis);
                        value = readString(dis);
                        hashMap.put(key, value);
                        expirationMap.put(key, expireTimeSeconds * 1000);
                        break;
                    case 0xFC: // Expired key (milliseconds)
                        long expireTimeMilliseconds = dis.readLong();
                        dataType = dis.readUnsignedByte();
                        key = readString(dis);
                        value = readString(dis);
                        hashMap.put(key, value);
                        expirationMap.put(key, expireTimeMilliseconds);
                        break;
                    case 0xFE: // Select DB
                        int dbIndex = dis.readUnsignedByte();
                        break;
                    case 0xFF: // End of file
                        break;
                    default:
                        throw new IOException("Unsupported RDB data type: " + dataType);
                }
            }
        } catch (IOException e) {
            Logger.getLogger(AsyncServer.class.getName()).log(Level.SEVERE, "Error parsing RDB file", e);
            throw e;
        }
        this.memory.putAll(hashMap);
        this.expiration.putAll(expirationMap);
        return hashMap;
    }

    private String readString(DataInputStream dis) throws IOException {
        int length = dis.readInt();
        byte[] buffer = new byte[length];
        dis.readFully(buffer);
        return new String(buffer, StandardCharsets.UTF_8);
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
