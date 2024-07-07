package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.util.List;
import java.util.logging.Logger;

public class GetCommand extends RedisCommand {
    private static final Logger LOGGER = Logger.getLogger(GetCommand.class.getName());

    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        String key = command.get(1);
        Long expiration = handler.expiration.get(key);

        // Check if the key exists and if it has not expired
        if (expiration != null) {
            long currentTime = System.currentTimeMillis();
            if (expiration < currentTime) {
                handler.memory.remove(key);
                handler.expiration.remove(key);
                LOGGER.info("Key '" + key + "' expired at " + expiration + " (current time: " + currentTime + ") and removed from memory.");
                return "$-1\r\n";
            }
        }

        String value = handler.memory.get(key);
        if (value != null) {
            LOGGER.info("Key '" + key + "' found with value: " + value);
            return EncodingUtils.createRedisResponse(value);
        } else {
            LOGGER.info("Key '" + key + "' not found.");
            return "$-1\r\n";
        }
    }
}
