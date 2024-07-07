package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.util.List;

public class GetCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        if (handler.expiration.containsKey(command.get(1)) && handler.expiration.get(command.get(1)) < System.currentTimeMillis() / 1000) {
            handler.memory.remove(command.get(1));
            handler.expiration.remove(command.get(1));
            return "$-1\r\n";
        } else {
            String value = handler.memory.get(command.get(1));
            if (value != null) {
                return EncodingUtils.createRedisResponse(value);
            } else {
                return "$-1\r\n";
            }
        }
    }
}
