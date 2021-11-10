package mains;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

/**
 * Created by catherine on 17/07/2016.
 */
public class OutputMain {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("OutputMain QUEUE");
            System.exit(1);
        }
        String path = args[0];
        SingleChronicleQueue queue = SingleChronicleQueueBuilder.binary(path).build();
        ExcerptTailer tailer = queue.createTailer();

        while (true) {
            long index = tailer.index();
            String text = tailer.readText();
            if (text == null) {
                Jvm.pause(10);
            } else {
                System.out.println("[" + index + "] " + text);
            }
        }

    }
}
