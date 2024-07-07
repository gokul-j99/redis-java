package commands;

import utils.AsyncRequestHandler;

import java.io.BufferedWriter;
import java.util.List;

public class WaitCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        int maxWaitMs = Integer.parseInt(command.get(2));
        int numReplicas = Integer.parseInt(command.get(1));
        for (BufferedWriter writer : handler.server.getWriters()) {
            writer.write("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
            writer.flush();
        }

        long startTime = System.currentTimeMillis();
        while (handler.server.numacks < numReplicas && (System.currentTimeMillis() - startTime) < maxWaitMs) {
            System.out.println("NUMACKS: " + handler.server.numacks + " num_replicas: " + numReplicas +
                    " max_wait_ms: " + maxWaitMs + " time: " + System.currentTimeMillis() + " start_time: " + startTime);
            Thread.sleep(100);
        }
        System.out.println("SENDING BACK " + handler.server.numacks);
        return ":" + handler.server.numacks + "\r\n";
    }
}
