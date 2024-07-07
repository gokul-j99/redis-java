package commands;

import utils.AsyncRequestHandler;

import java.io.OutputStream;
import java.util.List;

public class PSyncCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) throws Exception {
        // Generate a full resync response
        String response = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n";

        // Dummy RDB data for example purposes
        String rdbHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        byte[] binaryData = hexStringToByteArray(rdbHex);

        String header = "$" + binaryData.length + "\r\n";

        // Write response to the client
        handler.getWriter().write(response);
        handler.getWriter().write(header);
        handler.getWriter().flush();  // Flush character data

        // Write binary data directly using OutputStream
        OutputStream outputStream = handler.getOutputStream();
        outputStream.write(binaryData);
        outputStream.flush();  // Flush binary data

        handler.server.numacks += 1;

        return ""; // Returning an empty string to indicate no further response is needeed
    }

    private static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }
}
