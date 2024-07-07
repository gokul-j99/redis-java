package commands;

import utils.AsyncRequestHandler;

import java.util.List;

import static utils.Constants.DISCARD_WITHOUT_MULTI;

public class DiscardCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        if (handler.commandQueue == null) {
            return DISCARD_WITHOUT_MULTI;
        }
        handler.commandQueue = null;
        return "+OK\r\n";
    }
}

