package commands;

import utils.AsyncRequestHandler;

import java.util.List;

public class PingCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        return "+PONG\r\n";
    }
}
