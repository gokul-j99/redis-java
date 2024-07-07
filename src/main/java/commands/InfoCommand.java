package commands;

import utils.AsyncRequestHandler;
import utils.EncodingUtils;

import java.util.List;

public class InfoCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        if ("replication".equalsIgnoreCase(command.get(1))) {
            if (handler.replicaServer == null) {
                String masterReplid = EncodingUtils.generateRandomString(40);
                String masterReplOffset = "0";
                String payload = "role:master\nmaster_replid:" + masterReplid + "\nmaster_repl_offset:" + masterReplOffset;
                String response = EncodingUtils.asBulkString(payload);
                return response;
            } else {
                return "+role:slave\r\n";
            }
        } else {
            return "-ERR unknown INFO section\r\n";
        }
    }
}
