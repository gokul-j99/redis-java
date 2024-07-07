package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.util.List;

public class GetCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        String key = command.get(1);
        Long expiration = handler.expiration.get(key);

        // Check if the key exists and if it has not expired
        if (expiration != null && expiration < System.currentTimeMillis() / 1000) {
            handler.memory.remove(key);
            handler.expiration.remove(key);
            return "$-1\r\n";
        }

        String value = handler.memory.get(key);
        if (value != null) {
            return EncodingUtils.createRedisResponse(value);
        } else {
            return "$-1\r\n";
        }
    }
}
