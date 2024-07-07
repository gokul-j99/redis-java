package commands;

import utils.AsyncRequestHandler;
import utils.StreamUtils;

import java.util.List;
import java.util.Map;

public class XReadCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        List<String> streamKeys = null;
        List<String> streamIds = null;
        if (command.get(1).equalsIgnoreCase("block")) {
            int timeout = Integer.parseInt(command.get(2));
            List<List<String>> result = (List<List<String>>) StreamUtils.blockRead(timeout, command, handler.server);
            streamKeys = result.get(0);
            streamIds = result.get(1);
        }

        if (streamKeys == null || streamIds == null) {
            Map<String, List<String>> keysAndIds = (Map<String, List<String>>) StreamUtils.getStreamKeysAndIds(command, handler.server);
            streamKeys = keysAndIds.get("keys");
            streamIds = keysAndIds.get("ids");
        }

        StringBuilder retString = new StringBuilder("*" + streamKeys.size() + "\r\n");
        for (int i = 0; i < streamKeys.size(); i++) {
            String streamKey = streamKeys.get(i);
            String streamId = streamIds.get(i);
            retString.append(StreamUtils.getOneXreadResponse(streamKey, streamId, handler.server));
        }
        return retString.toString();
    }
}
