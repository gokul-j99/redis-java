package utils;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;
import commands.*;

public class AsyncRequestHandler implements Runnable {
    public final Socket socket;
    public final AsyncServer server;
    public final BufferedReader reader;
    public final BufferedWriter writer;
    public final OutputStream outputStream;
    public final Map<String, String> memory;
    public final Map<String, Long> expiration;
    public final String replicaServer;
    public final int replicaPort;
    public int offset = 0;
    public BlockingQueue<List<String>> commandQueue = null;

    public final Map<String, RedisCommand> commandMap = new HashMap<>();

    public AsyncRequestHandler(Socket socket, AsyncServer server) throws IOException {
        this.socket = socket;
        this.server = server;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
        this.outputStream = socket.getOutputStream();
        this.memory = server.getMemory();
        this.expiration = server.getExpiration();
        this.replicaServer = server.getReplicaServer();
        this.replicaPort = server.getReplicaPort();

        // Initialize commandMap with command instances
        commandMap.put("PING", new PingCommand());
        commandMap.put("ECHO", new EchoCommand());
        commandMap.put("SET", new SetCommand());
        commandMap.put("GET", new GetCommand());
        commandMap.put("INFO", new InfoCommand());
        commandMap.put("REPLCONF", new ReplConfCommand());
        commandMap.put("PSYNC", new PSyncCommand());
        commandMap.put("WAIT", new WaitCommand());
        commandMap.put("CONFIG", new ConfigCommand());
        commandMap.put("KEYS", new KeysCommand());
        commandMap.put("TYPE", new TypeCommand());
        commandMap.put("XADD", new XAddCommand());
        commandMap.put("XRANGE", new XRangeCommand());
        commandMap.put("XREAD", new XReadCommand());
        commandMap.put("INCR", new IncrCommand());
        commandMap.put("INCRBY", new IncrByCommand());
        commandMap.put("EXEC", new ExecCommand());
        commandMap.put("MULTI", new MultiCommand());
        commandMap.put("DISCARD", new DiscardCommand());
    }

    @Override
    public void run() {
        try {
            processRequest();
        } catch (IOException e) {
            Logger.getLogger(AsyncRequestHandler.class.getName()).log(Level.SEVERE, null, e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void processRequest() throws Exception {
        while (true) {
            char[] buffer = new char[1024];
            int read = reader.read(buffer);
            if (read == -1) {
                break;
            }
            String request = new String(buffer, 0, read);
            System.out.println(request);
            Logger.getLogger(AsyncRequestHandler.class.getName()).info("Request: " + request);
            handleRequest(request.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void handleRequest(byte[] request) throws Exception {
        List<List<String>> commandList = EncodingUtils.parseRedisProtocol(request);
        List<Integer> lengths = EncodingUtils.getCommandLengths(request);

        if (commandList.isEmpty()) {
            Logger.getLogger(AsyncRequestHandler.class.getName()).info("Received invalid data");
            System.out.println("Received invalid data");
            return;
        }

        for (int i = 0; i < commandList.size(); i++) {
            List<String> cmd = commandList.get(i);
            String cmdName = cmd.get(0).toUpperCase(); // Command names are case-insensitive
            RedisCommand commandClass = commandMap.getOrDefault(cmdName, new UnknownCommand());

            if (commandQueue != null && !Arrays.asList("MULTI", "EXEC", "DISCARD").contains(cmdName)) {
                commandQueue.add(cmd);
                writer.write("+QUEUED\r\n");
                writer.flush();
                return;
            }

            String response;
            if (commandMap.containsKey(cmdName)) {
                response = commandClass.execute(this, cmd);
            } else {
                response = new UnknownCommand().execute(this, cmd);
            }

            if (replicaServer != null && socket != null && socket.getPort() == replicaPort) {
                if (response.startsWith("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK")) {
                    writer.write(response);
                    writer.flush();
                }
                offset += lengths.get(i);
            } else {
                if (response != null) {
                    System.out.println("sending response: " + response + " to " + socket.getRemoteSocketAddress() + " command: " + cmd);
                    writer.write(response);
                    writer.flush();
                }
                offset += lengths.get(i);
            }
        }
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    public AsyncServer getServer() {
        return server;
    }

    public Integer getOffset() {
        return offset;
    }


}
