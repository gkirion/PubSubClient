package main;

import com.george.pubsub.client.PubSubClient;
import com.george.pubsub.client.RemoteSubscriber;
import com.george.pubsub.remote.RemoteAddress;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println(args);
        RemoteAddress localAddress = new RemoteAddress("localhost", Integer.parseInt(args[0]));
        RemoteSubscriber subscriber = new RemoteSubscriber(localAddress.getIp(), localAddress.getPort());
        PubSubClient pubSubClient = new PubSubClient("localhost", 15000);
        final int[] messagesReceived = {0};
        subscriber.addReceiver(new Receivable() {
            @Override
            public void receive(Message message) {

                System.out.println(message);
                messagesReceived[0]++;
            }
        });
        pubSubClient.subscribe("test", localAddress);
        for (int i = 0; i < 800; i++) {
            pubSubClient.publish("test", "hello george!");
        }
        System.out.println("messages sent: " + 800);
        System.out.println("messages received: " + messagesReceived[0]);
    }

}
