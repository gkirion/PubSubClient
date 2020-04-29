package com.george.pubsub.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.george.http.HttpBuilder;
import com.george.http.HttpResponse;
import com.george.pubsub.remote.RemoteAddress;
import com.george.pubsub.remote.RemoteSubscription;
import pubsub.broker.Message;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class PubSubClient {

    private String remoteBrokerIp;
    private int remoteBrokerPort;
    private ObjectMapper mapper;
    private Set<String> subscriptions;

    public PubSubClient(String remoteBrokerIp) {
        this(remoteBrokerIp, 80);
    }

    public PubSubClient(String remoteBrokerIp, int remoteBrokerPort) {
        this.remoteBrokerIp = remoteBrokerIp;
        this.remoteBrokerPort = remoteBrokerPort;
        mapper = new ObjectMapper();
        subscriptions = new HashSet<>();
    }

    public void publish(String topic, String text) {
        try {
            Message message = new Message(topic, text);
            HttpResponse response = HttpBuilder.get(remoteBrokerIp, remoteBrokerPort, "publish")
                    .body(mapper.writeValueAsString(message)).send();
            //System.out.println(response);
        } catch (IOException e) {
            System.out.println("could not connect to the broker server...");
            System.out.println("reason: " + e.getMessage());
        }
    }

    public boolean subscribe(String topic, RemoteAddress localAddress) {
        try {
            RemoteSubscription remoteSubscription = new RemoteSubscription();
            remoteSubscription.setTopic(topic);
            remoteSubscription.setRemoteSubscriber(localAddress);
            HttpResponse response = HttpBuilder.get(remoteBrokerIp, remoteBrokerPort, "subscribe")
                    .body(mapper.writeValueAsString(remoteSubscription)).send();

            if (response.getResponseCode() == 200) {
                subscriptions.add(topic);
                return true;
            }
        } catch (IOException e) {
            System.out.println("could not connect to remote broker...");
            System.out.println("reason: " + e.getMessage());
        }
        return false;
    }

}
