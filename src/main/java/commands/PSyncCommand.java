package commands;

import utils.AsyncRequestHandler;

import java.util.List;

public class PSyncCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        String response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";
        String rdbHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        byte[] binaryData = hexStringToByteArray(rdbHex);
        String header = "$" + binaryData.length + "\r\n";
        handler.writer.write(String.valueOf(response.getBytes()));
        handler.writer.write(String.valueOf(header.getBytes()));
        handler.writer.write(String.valueOf(binaryData));
        handler.writer.flush();
        handler.server.numacks++;
        return "";
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
