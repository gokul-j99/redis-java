package commands;

import utils.AsyncRequestHandler;

import java.util.List;

public
class KeysCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        return String.join("\r\n", handler.server.getKeysArray()) + "\r\n";
    }
}
