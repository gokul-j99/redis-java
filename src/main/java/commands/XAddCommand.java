package commands;

import utils.AsyncRequestHandler;
import utils.StreamUtils;

import java.util.HashMap;
import java.util.List;

public
class XAddCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        String streamKey = command.get(1);
        String streamId = command.get(2);
        streamId = StreamUtils.generateStreamId(streamKey, streamId, handler.server);
        String errMessage = StreamUtils.validateStreamId(streamKey, streamId, handler.server);
        if (errMessage != null) {
            return errMessage;
        }
        if (!handler.server.streamstore.containsKey(streamKey)) {
            handler.server.streamstore.put(streamKey, new HashMap<>());
        }
        String[] streamIdParts = streamId.split("-");
        int entryNumber = Integer.parseInt(streamIdParts[0]);
        int sequenceNumber = Integer.parseInt(streamIdParts[1]);
        if (!handler.server.streamstore.get(streamKey).containsKey(entryNumber)) {
            handler.server.streamstore.get(streamKey).put(entryNumber, new HashMap<>());
        }
        handler.server.streamstore.get(streamKey).get(entryNumber).put(sequenceNumber, command.subList(3, command.size()));
        return "$" + streamId.length() + "\r\n" + streamId + "\r\n";
    }
}