package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.util.ArrayList;
import java.util.List;

import static utils.Constants.EXEC_WITHOUT_MULTI;

public class ExecCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        if (handler.commandQueue == null) {
            return EXEC_WITHOUT_MULTI;
        }

        List<String> responses = new ArrayList<>();
        while (!handler.commandQueue.isEmpty()) {
            List<String> queuedCommand = handler.commandQueue.poll();
            System.out.println("EXECUTING COMMAND: " + queuedCommand);
            String commandName = queuedCommand.get(0).toUpperCase();
            RedisCommand commandToExec = handler.commandMap.getOrDefault(commandName, new UnknownCommand());
            responses.add(commandToExec.execute(handler, queuedCommand));
        }
        handler.commandQueue = null;
        return EncodingUtils.generateRedisArray(responses);
    }
}