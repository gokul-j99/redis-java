package commands;

import utils.AsyncRequestHandler;

import java.util.List;

public class UnknownCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        return "-ERR unknown command\r\n";
    }
}
