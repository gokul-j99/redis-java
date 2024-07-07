import java.io.IOException;
import java.util.logging.*;
import java.util.concurrent.*;
import org.apache.commons.cli.*;
import utils.AsyncServer;

public class Main {
    private static int pingCount = 0;

    public static void main(String[] args) {
        Logger logger = Logger.getLogger(Main.class.getName());
        logger.setLevel(Level.INFO);

        Options options = new Options();

        Option portOption = new Option("p", "port", true, "Port to run the server on");
        portOption.setRequired(false);
        portOption.setType(Number.class);
        options.addOption(portOption);

        Option replicaOption = new Option("r", "replicaof", true, "Replicate data from a master server");
        replicaOption.setRequired(false);
        options.addOption(replicaOption);

        Option dirOption = new Option("d", "dir", true, "Path to the directory where the RDB file is stored");
        dirOption.setRequired(false);
        options.addOption(dirOption);

        Option dbFilenameOption = new Option("f", "dbfilename", true, "Name of the RDB file");
        dbFilenameOption.setRequired(false);
        options.addOption(dbFilenameOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Async Redis-like Server", options);
            System.exit(1);
            return;
        }

        int port = Integer.parseInt(cmd.getOptionValue("port", "6379"));
        String replicaOf = cmd.getOptionValue("replicaof", null);
        String dir = cmd.getOptionValue("dir", "");
        String dbFilename = cmd.getOptionValue("dbfilename", "");

        String replicaServer = null;
        int replicaPort = 0;

        if (replicaOf != null) {
            String[] replicaInfo = replicaOf.split(" ");
            if (replicaInfo.length != 2) {
                throw new IllegalArgumentException("Invalid replicaof argument. Must be in the format 'server port'");
            }
            replicaServer = replicaInfo[0];
            replicaPort = Integer.parseInt(replicaInfo[1]);
        }

        try {
            AsyncServer.create("127.0.0.1", port, replicaServer, replicaPort, dir, dbFilename);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to start server", e);
            System.exit(1);
        }
    }
}
