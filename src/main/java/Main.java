import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.cli.*;
public class Main {
    private static final int DEFAULT_PORT = 6379;
    private static final RedisServerRole DEFAULT_ROLE = RedisServerRole.MASTER;
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
            String hostname = replicaArgsArray[0];
            int masterPortNumber = Integer.parseInt(replicaArgsArray[1]);
            System.out.println("Replicating from master, master host is: " +
                    hostname + ", port number is: " + masterPortNumber);
            role = RedisServerRole.SLAVE;
            replicaHandshake(hostname, masterPortNumber);
        }
        RedisDatastore datastore = new RedisDatastore();
        ServerSocket serverSocket = new ServerSocket(portNumber);
        System.out.println("Listening for connections on port: " + portNumber);
        // Since the tester restarts your program quite often, setting SO_REUSEADDR
        // ensures that we don't run into 'Address already in use' errors
        serverSocket.setReuseAddress(true);
        // For handling multiple connections
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        while (true) {
            // Wait for connection from client.
            Socket clientSocket = serverSocket.accept();
            executorService.submit(
                    new ProcessRequestRunnable(clientSocket, datastore, role));
        }
    }
    private static void replicaHandshake(String hostname, int masterPortNumber)
            throws IOException {
        String request = "*1\r\n$4\r\nping\r\n";
        try (Socket socket = new Socket(hostname, masterPortNumber);
             DataOutputStream dos =
                     new DataOutputStream(socket.getOutputStream())) {
            dos.write(request.getBytes(StandardCharsets.UTF_8));
            dos.flush();
        }
    }
    public static class ProcessRequestRunnable implements Runnable {
        private final Socket clientSocket;
        private final RedisDatastore datastore;
        private final RedisServerRole role;
        public ProcessRequestRunnable(Socket clientSocket, RedisDatastore datastore,
                                      RedisServerRole role) {
            this.clientSocket = clientSocket;
            this.datastore = datastore;
            this.role = role;
        }
        @Override
        public void run() {
            try (DataOutputStream dos =
                         new DataOutputStream(clientSocket.getOutputStream());
                 DataInputStream dis =
                         new DataInputStream(clientSocket.getInputStream());) {
                while (true) {
                    String request = readRequestFromInputStream(dis);
                    System.out.println("Request received: " + request);
                    RedisRequest redisRequest = RedisCommandParser.parse(request);
                    switch (redisRequest.redisCommand()) {
                        case PING
                                -> {
                            dos.write("+PONG\r\n".getBytes(StandardCharsets.UTF_8));
                            dos.flush();
                        } case ECHO
                                -> {
                            dos.write(getRespBulkString(redisRequest.elements().getFirst())
                                    .getBytes(StandardCharsets.UTF_8));
                            dos.flush();
                        } case SET
                                -> {
                            String key = redisRequest.elements().get(0);
                            String value = redisRequest.elements().get(1);
                            // todo refactor processing of get requests, create enum for
                            // arguments
                            if (redisRequest.elements().size() > 2) {
                                if (redisRequest.elements().get(2).equalsIgnoreCase("px")) {
                                    long ms = Long.parseLong(redisRequest.elements().get(3));
                                    datastore.set(key, value, ms);
                                }
                            } else {
                                datastore.set(key, value);
                            }
                            dos.write("+OK\r\n".getBytes(StandardCharsets.UTF_8));
                            dos.flush();
                        } case GET
                                -> {
                            String key = redisRequest.elements().get(0);
                            Optional<String> value = datastore.get(key);
                            if (value.isPresent()) {
                                // GET foo
                                // $3\r\nbar\r\n
                                String response = getRespBulkString(value.get());
                                System.out.println("Responding to get command with: " + response);
                                dos.write(response.getBytes(StandardCharsets.UTF_8));
                            } else {
                                dos.write("$-1\r\n".getBytes(StandardCharsets.UTF_8));
                            } dos.flush();
                        } case INFO
                                -> {
                            // $11\r\nrole:master\r\n
                            String response = role == RedisServerRole.MASTER ? "role:master":
                                    "role:slave";
                            response +=
                                    "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
                            response += "master_repl_offset:0";
                            String bulkResponse = getRespBulkString(response);
                            System.out.println("Responding to get command with: " +
                                    bulkResponse);
                            dos.write(bulkResponse.getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
            }
            catch (Exception e) {
                System.out.println(Thread.currentThread().getName() + " exception, " + e);
            }
        }
        private static String readRequestFromInputStream(DataInputStream in)
                throws IOException, InterruptedException {
            // Loop until data becomes available.
            while (in.available() == 0) {
                // Optionally, include a small sleep to avoid high CPU usage.
                Thread.sleep(10); // Sleep for 10 milliseconds.
            }
            // At this point, data is available. Read all available data into a byte
            // array.
            int availableBytes = in.available();
            byte[] buffer = new byte[availableBytes];
            int bytesRead = in.read(buffer);
            // If end of stream is reached, return null.
            if (bytesRead == -1) {
                return null;
            }
            // Convert the byte array to a String.
            return new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
        }
    }
    private static String getRespBulkString(String message) {
        int length = message.length();
        return String.format("$%s\r\n%s\r\n", length, message);
    }
    private static CommandLine parsePortFromCommandLineArgs(String[] args)
            throws ParseException {
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