package com.george.pubsub.client.main;

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
        subscriber.addReceiver(new Receivable() {
            @Override
            public void receive(Message message) {
                System.out.println(message);
            }
        });
        pubSubClient.subscribe("test", localAddress);
        pubSubClient.publish("test", "hello george!");
    }

}
