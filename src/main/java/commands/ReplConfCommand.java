package commands;

import utils.AsyncRequestHandler;

import java.io.BufferedWriter;
import java.util.List;

public class ReplConfCommand extends RedisCommand {
    @Override
    public String execute(AsyncRequestHandler handler, List<String> command) {
        BufferedWriter writer = handler.getWriter();  // Correct way to access the writer instance variable
        if (command.size() > 2 && "listening-port".equals(command.get(1))) {
            handler.getServer().getWriters().add(writer);
        } else if (command.size() > 2 && "GETACK".equals(command.get(1))) {
            String response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + handler.getOffset().toString().length() + "\r\n" + handler.getOffset() + "\r\n";
            System.out.println("REPLCONF ACK: " + response);
            return response;
        } else if (command.size() > 2 && "ACK".equals(command.get(1))) {
            System.out.println("Incrementing num acks");
            handler.getServer().incrementNumAcks();
            return "";
        }
        return "+OK\r\n";
    }
}
