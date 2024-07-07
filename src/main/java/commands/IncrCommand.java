package commands;

import utils.AsyncRequestHandler;

import java.util.Arrays;
import java.util.List;

public class IncrCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        IncrByCommand incrByCommand = new IncrByCommand();
        return incrByCommand.execute(handler, Arrays.asList("INCRBY", command.get(1), "1"));
    }
}
