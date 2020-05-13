package com.george.pubsub.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.george.http.HttpBuilder;
import com.george.http.HttpResponse;
import com.george.pubsub.remote.RemoteAddress;
import com.george.pubsub.remote.RemoteSubscription;
import pubsub.broker.Broker;
import pubsub.broker.Brokerable;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PubSubClient implements Brokerable {

    private String ip;
    private int port;
    private ServerSocket serverSocket;
    private ExecutorService executor;
    private String remoteBrokerIp;
    private int remoteBrokerPort;
    private ObjectMapper mapper;
    private Broker broker;
    private Set<String> subscriptions;

    public PubSubClient(String ip) throws IOException {
        this(ip, 80);
    }

    public PubSubClient(int port) throws IOException {
        this("localhost", port);
    }

    public PubSubClient(String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        mapper = new ObjectMapper();
        subscriptions = new HashSet<>();
        broker = new Broker();

        serverSocket = new ServerSocket(port, 50, InetAddress.getByName(ip));
        System.out.println("started PubSub client... listening on ip=" + serverSocket.getInetAddress() + " port=" + serverSocket.getLocalPort());
        executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        InputStreamReader inputStreamReader = new InputStreamReader(clientSocket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        String line = bufferedReader.readLine();
                        //System.out.println(line);
                        String type = line.split(" ")[1].substring(1);
                        int len = 0;
                        line = bufferedReader.readLine();
                        while (!line.isEmpty()) {
                            //                 System.out.println(line);
                            if (line.startsWith("Content-Length")) {
                                String[] tokens = line.split(":");
                                // System.out.println(tokens[0]);
                                //System.out.println(tokens[1]);
                                //System.out.println(len = Integer.parseInt(tokens[1].trim()));
                                len = Integer.parseInt(tokens[1].trim());
                            }
                            line = bufferedReader.readLine();
                        }
                        if (len > 0) {
                            char[] buf = new char[len];
                            bufferedReader.read(buf, 0, len);
                            //System.out.println(bufferedReader.read(buf, 0, len));
                            line = String.copyValueOf(buf);
                        }
                        if (type.equals("receive")) {
                            Message message = mapper.readValue(line, Message.class);
                            broker.publish(message);
                            clientSocket.getOutputStream().write("HTTP/1.1 200 OK\n\n".getBytes("UTF-8"));
                        } else {
                            clientSocket.getOutputStream().write("HTTP/1.1 404 Not Found\n\n".getBytes("UTF-8"));
                        }
                        clientSocket.close();
                    }
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
            }
        });

    }

    public String getRemoteBrokerIp() {
        return remoteBrokerIp;
    }

    public void setRemoteBrokerIp(String remoteBrokerIp) {
        this.remoteBrokerIp = remoteBrokerIp;
    }

    public int getRemoteBrokerPort() {
        return remoteBrokerPort;
    }

    public void setRemoteBrokerPort(int remoteBrokerPort) {
        this.remoteBrokerPort = remoteBrokerPort;
    }

    @Override
    public void publish(Message message) {
        try {
            HttpResponse response = HttpBuilder.get(remoteBrokerIp, remoteBrokerPort, "publish")
                    .body(mapper.writeValueAsString(message)).send();
            //System.out.println(response);
        } catch (IOException e) {
            System.out.println("could not connect to the broker server...");
            System.out.println("reason: " + e.getMessage());
        }
    }

    @Override
    public void publish(String topic, String text) {
        Message message = new Message(topic, text);
        publish(message);
    }

    @Override
    public boolean subscribe(String topic, Receivable receivable) {
        try {
            RemoteSubscription remoteSubscription = new RemoteSubscription();
            remoteSubscription.setTopic(topic);
            remoteSubscription.setRemoteSubscriber(new RemoteAddress(ip, port));
            HttpResponse response = HttpBuilder.get(remoteBrokerIp, remoteBrokerPort, "subscribe")
                    .body(mapper.writeValueAsString(remoteSubscription)).send();

            if (response.getResponseCode() == 200) {
                subscriptions.add(topic);
                return broker.subscribe(topic, receivable);
            }
        } catch (IOException e) {
            System.out.println("could not connect to remote broker...");
            System.out.println("reason: " + e.getMessage());
        }
        return false;
    }

    public void shutdown() throws IOException {
        serverSocket.close();
        executor.shutdown();
    }

}
