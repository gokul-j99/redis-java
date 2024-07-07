package commands;

import utils.AsyncRequestHandler;

import java.util.List;

public class TypeCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        String key = command.get(1);
        if (handler.memory.containsKey(key) &&
                (!handler.expiration.containsKey(key) || handler.expiration.get(key) >= System.currentTimeMillis() / 1000)) {
            return "+string\r\n";
        } else if (handler.server.streamstore.containsKey(key)) {
            return "+stream\r\n";
        } else {
            return "+none\r\n";
        }
    }
}