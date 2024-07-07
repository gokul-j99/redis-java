package commands;

import utils.AsyncRequestHandler;

import java.util.ArrayList;
import java.util.List;

public class ConfigCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        if (command.size() > 1) {
            List<String> configParams = command.subList(2, command.size());
            List<String> response = new ArrayList<>();
            for (String param : configParams) {
                response.add(param);
                if (handler.server.config.containsKey(param)) {
                    String value = handler.server.config.get(param);
                    response.add(value);
                } else {
                    response.add("(nil)");
                }
            }
            return handler.server.asArray(response);
        }
        return "";
    }
}
