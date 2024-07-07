package commands;

import utils.AsyncRequestHandler;
import utils.StreamUtils;
import utils.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XRangeCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        String streamKey = command.get(1);
        String lower = command.get(2);
        String upper = command.get(3);
        if (lower.equals("-")) {
            lower = "0-0";
        }

        String noneString = "+none\r\n";
        if (!handler.server.streamstore.containsKey(streamKey)) {
            System.out.println("Stream key '" + streamKey + "' not found in streamstore");
            return noneString;
        }

        Map<Integer, Map<Integer, List<String>>> streamstore = handler.server.streamstore.get(streamKey);
        List<Integer> keys = new ArrayList<>(streamstore.keySet());
        if (upper.equals("+")) {
            upper = keys.get(keys.size() - 1) + "-" + streamstore.get(keys.get(keys.size() - 1)).size();
        }

        String[] lowerParts = lower.split("-");
        String[] upperParts = upper.split("-");
        int lowerOuter = Integer.parseInt(lowerParts[0]);
        int lowerInner = Integer.parseInt(lowerParts[1]);
        int upperOuter = Integer.parseInt(upperParts[0]);
        int upperInner = Integer.parseInt(upperParts[1]);

        Tuple<Integer, Integer> tuple = StreamUtils.findOuterIndices(keys, lowerOuter,upperOuter);
        int startIndex = tuple.getFirst();
        int endIndex = tuple.getSecond();
        System.out.println("Start index: " + startIndex + ", End index: " + endIndex);

        if (startIndex == -1 || endIndex == -1 || startIndex >= keys.size() || endIndex < 0 || startIndex > endIndex) {
            System.out.println("Invalid range indices");
            return noneString;
        }

        int streamstoreStartIndex = StreamUtils.findInnerStartIndex(streamstore, keys, startIndex, lowerOuter, lowerInner);
        int streamstoreEndIndex = StreamUtils.findInnerEndIndex(streamstore, keys, endIndex, upperOuter, upperInner);
        System.out.println("Streamstore start index: " + streamstoreStartIndex + ", Streamstore end index: " + streamstoreEndIndex);

        if (streamstoreStartIndex == -1 || streamstoreEndIndex == -1) {
            System.out.println("Invalid inner indices");
            return noneString;
        }

        Map<String, List<String>> elements = StreamUtils.extractElements(streamstore, keys, startIndex, endIndex, streamstoreStartIndex, streamstoreEndIndex);
        StringBuilder retString = new StringBuilder("*" + elements.size() + "\r\n");
        for (Map.Entry<String, List<String>> entry : elements.entrySet()) {
            String key = entry.getKey();
            List<String> value = entry.getValue();
            retString.append("*2\r\n$").append(key.length()).append("\r\n").append(key).append("\r\n");
            retString.append(handler.server.asArray(value));
        }
        System.out.println("Ret string: " + retString);
        return retString.toString();
    }
}
