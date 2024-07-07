package utils;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
import java.util.stream.Collectors;

public class StreamUtils {

    public static String validateStreamId(String streamKey, String streamId, AsyncServer server) {
        if (streamId.compareTo("0-0") <= 0) {
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }

        if (!server.getStreamstore().containsKey(streamKey)) {
            return "";
        }

        int lastEntryNumber = server.getStreamstore().get(streamKey).keySet().stream().max(Integer::compareTo).orElse(0);
        System.out.println("Last entry number: " + lastEntryNumber);
        int lastEntrySequence = server.getStreamstore().get(streamKey).get(lastEntryNumber).keySet().stream().max(Integer::compareTo).orElse(0);

        int currentEntryNumber = Integer.parseInt(streamId.split("-")[0]);
        int currentEntrySequence = Integer.parseInt(streamId.split("-")[1]);

        String errString = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        if (currentEntryNumber < lastEntryNumber) {
            return errString;
        } else if (currentEntryNumber == lastEntryNumber && currentEntrySequence <= lastEntrySequence) {
            return errString;
        }
        return "";
    }

    public static String generateStreamId(String streamKey, String streamId, AsyncServer server) {
        String[] parts = streamId.split("-");

        if (isValidId(parts)) {
            return streamId;
        }

        if (isTimeStar(parts)) {
            return generateTimeStarId(streamKey, parts, server);
        }

        if (streamId.equals("*") || (parts[0].equals("*") && parts[1].equals("*"))) {
            return generateStarId(streamKey, server);
        }

        return "";
    }
    private static boolean isValidId(String[] parts) {
        return parts.length == 2 && parts[0].matches("\\d+") && parts[1].matches("\\d+");
    }

    private static boolean isTimeStar(String[] parts) {
        return parts.length == 2 && parts[0].matches("\\d+") && parts[1].equals("*");
    }

    private static int calculateSequenceNumber(String streamKey, int timestamp, AsyncServer server) {
        if (server.getStreamstore().containsKey(streamKey)) {
            Integer lastEntryNumber = server.getStreamstore().get(streamKey).keySet().stream()
                    .map(Integer::valueOf).max(Integer::compare).orElse(0);
            Integer lastEntrySequence = server.getStreamstore().get(streamKey).get(lastEntryNumber.toString()).keySet().stream()
                    .map(Integer::valueOf).max(Integer::compare).orElse(0);
            if (lastEntryNumber < timestamp) {
                return 0;
            } else {
                return lastEntrySequence + 1;
            }
        } else {
            return timestamp == 0 ? 1 : 0;
        }
    }


    public static String getLastStreamId(String streamKey, AsyncServer server) {
        if (server.getStreamstore().containsKey(streamKey)) {
            Map<Integer, Map<Integer, List<String>>> streamstore = server.getStreamstore().get(streamKey);
            if (!streamstore.isEmpty()) {
                Integer lastEntryNumber = streamstore.keySet().stream().max(Integer::compareTo).orElse(0);
                Map<Integer, List<String>> lastEntry = streamstore.get(lastEntryNumber);
                Integer lastEntrySequence = lastEntry.keySet().stream().max(Integer::compareTo).orElse(0);
                return lastEntryNumber + "-" + lastEntrySequence;
            }
        }
        return "";
    }


    private static String generateTimeStarId(String streamKey, String[] parts, AsyncServer server) {
        int entryNumber = Integer.parseInt(parts[0]);
        int sequenceNumber = calculateSequenceNumber(streamKey, entryNumber, server);
        return entryNumber + "-" + sequenceNumber;
    }

    private static String generateStarId(String streamKey, AsyncServer server) {
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        int sequenceNumber = calculateSequenceNumber(streamKey, currentTime, server);
        return currentTime + "-" + sequenceNumber;
    }


    public static CompletableFuture<Tuple<List<String>, List<String>>> blockRead(int blockTime, List<String> command, AsyncServer server) {
        return CompletableFuture.supplyAsync(() -> {
            List<String> streamKeys = new ArrayList<>();
            List<String> streamIds = new ArrayList<>();

            if (blockTime > 0) {
                try {
                    TimeUnit.MILLISECONDS.sleep(blockTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                boolean found = false;
                while (!found) {
                    for (int i = 0; i < streamKeys.size(); i++) {
                        String response = getOneXreadResponse(streamKeys.get(i), streamIds.get(i), server);
                        if (!response.equals("$-1\r\n")) {
                            found = true;
                            break;
                        }
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(50);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            return new Tuple<>(streamKeys, streamIds);
        });
    }

    public static Tuple<List<String>, List<String>> getStreamKeysAndIds(List<String> command, AsyncServer server) {
        List<String> streamKeys = new ArrayList<>();
        List<String> streamIds = new ArrayList<>();
        int startIndex = 2;
        if (command.get(1).equalsIgnoreCase("block")) {
            startIndex += 2;
        }
        if (command.get(command.size() - 1).equals("$")) {
            streamKeys = command.subList(startIndex, command.indexOf(command.stream().filter(x -> x.matches("\\$")).findFirst().orElse("")));
            streamIds = streamKeys.stream().map(streamKey -> getLastStreamId(streamKey, server)).collect(Collectors.toList());
        } else {
            streamKeys = command.subList(startIndex, command.indexOf(command.stream().filter(x -> x.matches("\\d+-\\d+")).findFirst().orElse("")));
            streamIds = command.stream().filter(x -> x.matches("\\d+-\\d+")).collect(Collectors.toList());
        }
        return new Tuple<>(streamKeys, streamIds);
    }

    public static String getOneXreadResponse(String streamKey, String streamId, AsyncServer server) {
        String[] streamIdParts = streamId.split("-");
        int entryNumber = Integer.parseInt(streamIdParts[0]);
        int sequenceNumber = Integer.parseInt(streamIdParts[1]);
        String noneString = "$-1\r\n";

        if (!server.getStreamstore().containsKey(streamKey)) {
            return noneString;
        }

        Map<Integer, Map<Integer, List<String>>> streamstore = server.getStreamstore().get(streamKey);

        if (streamstore.containsKey(entryNumber) && streamstore.get(entryNumber).containsKey(sequenceNumber)) {
            sequenceNumber++;
        }

        List<Integer> keys = streamstore.keySet().stream().sorted().collect(Collectors.toList());
        String upper = keys.get(keys.size() - 1) + "-" + streamstore.get(keys.get(keys.size() - 1)).keySet().stream().max(Integer::compareTo).orElse(0);

        int lowerOuter = entryNumber, lowerInner = sequenceNumber;
        int upperOuter = Integer.parseInt(upper.split("-")[0]), upperInner = Integer.parseInt(upper.split("-")[1]);

        Tuple<Integer, Integer> outerIndices = findOuterIndices(keys, lowerOuter, upperOuter);
        int startIndex = outerIndices.getFirst();
        int endIndex = outerIndices.getSecond();

        System.out.println("Start index: " + startIndex + ", End index: " + endIndex);

        if (startIndex == -1 || endIndex == -1 || startIndex >= keys.size() || endIndex < 0 || startIndex > endIndex) {
            System.out.println("Invalid range indices");
            return noneString;
        }

        int streamstoreStartIndex = findInnerStartIndex(streamstore, keys, startIndex, lowerOuter, lowerInner);
        int streamstoreEndIndex = findInnerEndIndex(streamstore, keys, endIndex, upperOuter, upperInner);

        System.out.println("Streamstore start index: " + streamstoreStartIndex + ", Streamstore end index: " + streamstoreEndIndex);

        if (streamstoreStartIndex == -1 || streamstoreEndIndex == -1) {
            System.out.println("Invalid inner indices");
            return noneString;
        }

        Map<String, List<String>> elements = extractElements(streamstore, keys, startIndex, endIndex, streamstoreStartIndex, streamstoreEndIndex);
        StringBuilder retString = new StringBuilder("*2\r\n$").append(streamKey.length()).append("\r\n").append(streamKey).append("\r\n*").append(elements.size()).append("\r\n");
        for (Map.Entry<String, List<String>> entry : elements.entrySet()) {
            retString.append("*2\r\n$").append(entry.getKey().length()).append("\r\n").append(entry.getKey()).append("\r\n").append(server.asArray(entry.getValue()));
        }
        System.out.println("Ret string: " + retString.toString());
        return retString.toString();
    }




    public static Tuple<Integer, Integer> findOuterIndices(List<Integer> keys, int lowerOuter, int upperOuter) {
        int startIndex = Collections.binarySearch(keys, lowerOuter);
        int endIndex = Collections.binarySearch(keys, upperOuter);
        if (startIndex < 0) startIndex = -startIndex - 1;
        if (endIndex < 0) endIndex = -endIndex - 2;
        if (startIndex >= keys.size() || endIndex < 0) {
            return new Tuple<>(-1, -1);
        }
        return new Tuple<>(startIndex, endIndex);
    }

    public static int findInnerStartIndex(Map<Integer, Map<Integer, List<String>>> streamstore, List<Integer> keys, int startIndex, int lowerOuter, int lowerInner) {
        if (keys.get(startIndex).equals(lowerOuter)) {
            List<Integer> innerKeys = new ArrayList<>(streamstore.get(keys.get(startIndex)).keySet());
            int streamstoreStartIndex = Collections.binarySearch(innerKeys, lowerInner);
            if (streamstoreStartIndex < 0) streamstoreStartIndex = -streamstoreStartIndex - 1;
            if (streamstoreStartIndex == innerKeys.size()) {
                startIndex++;
                if (startIndex >= keys.size()) {
                    return -1;
                }
                streamstoreStartIndex = 0;
            }
            return streamstoreStartIndex;
        } else {
            return 0;
        }
    }

    public static int findInnerEndIndex(Map<Integer, Map<Integer, List<String>>> streamstore, List<Integer> keys, int endIndex, int upperOuter, int upperInner) {
        if (keys.get(endIndex).equals(upperOuter)) {
            List<Integer> innerKeys = new ArrayList<>(streamstore.get(keys.get(endIndex)).keySet());
            int streamstoreEndIndex = Collections.binarySearch(innerKeys, upperInner);
            if (streamstoreEndIndex < 0) streamstoreEndIndex = -streamstoreEndIndex - 2;
            if (streamstoreEndIndex == -1) {
                endIndex--;
                if (endIndex < 0) {
                    return -1;
                }
                streamstoreEndIndex = innerKeys.size() - 1;
            }
            return streamstoreEndIndex;
        } else {
            return streamstore.get(keys.get(endIndex)).size() - 1;
        }
    }

    public static Map<String, List<String>> extractElements(Map<Integer, Map<Integer, List<String>>> streamstore, List<Integer> keys, int startIndex, int endIndex, int streamstoreStartIndex, int streamstoreEndIndex) {
        Map<String, List<String>> retDict = new HashMap<>();
        System.out.println("streamstore: " + streamstore + ", keys: " + keys + ", start_index: " + startIndex + ", end_index: " + endIndex + ", streamstore_start_index: " + streamstoreStartIndex + ", streamstore_end_index: " + streamstoreEndIndex);
        if (startIndex == endIndex) {
            Integer currentKey = keys.get(startIndex);
            List<Integer> streamstoreKeys = new ArrayList<>(streamstore.get(currentKey).keySet());
            Map<Integer, List<String>> currentElements = streamstore.get(currentKey);
            for (int i = streamstoreStartIndex; i <= streamstoreEndIndex; i++) {
                retDict.put(currentKey + "-" + streamstoreKeys.get(i), currentElements.get(streamstoreKeys.get(i)));
            }
        } else {
            for (int i = startIndex; i <= endIndex; i++) {
                Integer currentKey = keys.get(i);
                List<Integer> streamstoreKeys = new ArrayList<>(streamstore.get(currentKey).keySet());
                Map<Integer, List<String>> currentElements = streamstore.get(currentKey);
                for (int j = 0; j < currentElements.size(); j++) {
                    if ((i == startIndex && j < streamstoreStartIndex) || (i == endIndex && j > streamstoreEndIndex)) {
                        continue;
                    }
                    retDict.put(currentKey + "-" + streamstoreKeys.get(j), currentElements.get(streamstoreKeys.get(j)));
                }
            }
        }
        return retDict;
    }
}

