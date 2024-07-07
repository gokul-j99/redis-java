package commands;
import java.util.*;

import utils.AsyncRequestHandler;


public abstract class RedisCommand {
    public abstract String execute(AsyncRequestHandler handler, List<String> command) throws Exception;
}
































