package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.util.List;

import static utils.Constants.*;

public class IncrByCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        String key = command.get(1);
        String increment = command.get(2);
        String value = EncodingUtils.getValue(handler, key);
        if (value.equals(WRONG_TYPE_RESPONSE)) {
            return WRONG_TYPE_RESPONSE;
        }
        if (value.equals(NOT_FOUND_RESPONSE)) {
            value = "0";
        }
        try {
            int incrementValue = Integer.parseInt(increment);
            int currentValue = Integer.parseInt(value);
            handler.memory.put(key, String.valueOf(currentValue + incrementValue));
            return ":" + handler.memory.get(key) + "\r\n";
        } catch (NumberFormatException e) {
            return NON_INT_ERROR;
        }
    }
}
