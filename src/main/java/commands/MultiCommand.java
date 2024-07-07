package commands;

import utils.AsyncRequestHandler;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class MultiCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        if (handler.commandQueue == null) {
            handler.commandQueue = new LinkedBlockingQueue<>();
        }
        return "+OK\r\n";
    }
}
