package commands;

import utils.AsyncRequestHandler;
import java.util.List;

public class GetCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        String key = command.get(1);
        if (handler.expiration.containsKey(key) && handler.expiration.get(key) < System.currentTimeMillis() / 1000) {
            handler.memory.remove(key);
            handler.expiration.remove(key);
        }

        String value = handler.memory.get(key);
        if (value == null) {
            System.out.println("Key '" + key + "' not found..");
            return "$-1\r\n";
        } else {
            System.out.println("Key '" + key + "' found with value: " + value);
            return "$" + value.length() + "\r\n" + value + "\r\n";
        }
    }
}
