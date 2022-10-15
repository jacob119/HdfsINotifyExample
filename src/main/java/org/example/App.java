package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;

import java.io.IOException;
import java.net.URI;

public class App
{
    public static void main(String[] args) throws IOException, InterruptedException, MissingEventsException {

        long lastReadTxid = 0;

        if (args.length > 1) {
            lastReadTxid = Long.parseLong(args[0]);
        }

        System.out.println("Watching Start with = " + args[0]);
        HdfsAdmin admin = new HdfsAdmin(URI.create( args[0] ), new Configuration());

        DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();

        while (true) {
            EventBatch batch = eventStream.take();
            System.out.println("TxId = " + batch.getTxid());

            for (Event event : batch.getEvents()) {
                System.out.println("event type = " + event.getEventType());
                if (event.getEventType() == Event.EventType.CREATE) {
                    Event.CreateEvent createEvent = (Event.CreateEvent) event;
                    System.out.println("  path = " + createEvent.getPath());
                    System.out.println("  owner = " + createEvent.getOwnerName());
                    System.out.println("  ctime = " + createEvent.getCtime());
                }
            }

            Thread.sleep(5000);
        }
    }
}
