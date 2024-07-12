package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SetCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        System.out.println("Executing SET command: " + command);
        handler.memory.put(command.get(1), command.get(2));
        if (command.size() > 4 && "PX".equalsIgnoreCase(command.get(3)) && command.get(4).matches("\\d+")) {
            long expirationDuration = Long.parseLong(command.get(4)) / 1000; // Convert milliseconds to seconds
            handler.expiration.put(command.get(1), System.currentTimeMillis() / 1000 + expirationDuration);
        } else {
            handler.expiration.put(command.get(1), null);
        }
        handler.server.numacks = 0;

        byte[] encodedCommand = EncodingUtils.encodeRedisProtocol(command);
        String commandString = new String(encodedCommand, StandardCharsets.UTF_8);

        if (handler.socket != null && handler.socket.getPort() != handler.replicaPort) {
            for (BufferedWriter writer : handler.server.getWriters()) {
                writer.write(commandString);
                
                writer.flush();
            }
            return "+OK\r\n";
        } else {
            return null;
        }
    }
}
