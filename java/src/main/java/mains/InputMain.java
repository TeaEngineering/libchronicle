package mains;

import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

import java.util.Scanner;

/**
 * Created by catherine on 17/07/2016.
 */
public class InputMain {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("InputMain QUEUE");
            System.exit(1);
        }
        String path = args[0];
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
        ExcerptAppender appender = queue.acquireAppender();
        Scanner read = new Scanner(System.in);
        System.out.println("Input text:");
        while(read.hasNextLine()) {
            String line = read.nextLine();
            if (line.isEmpty())
                break;
            appender.writeText(line);
            System.out.println("[" + appender.lastIndexAppended() + "] " + line);
        }
        System.out.println("Input stream finished");
    }
}