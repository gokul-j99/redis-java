package commands;

import utils.AsyncRequestHandler;

import java.util.List;

public class EchoCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        return "+" + command.get(1) + "\r\n";
    }
}
