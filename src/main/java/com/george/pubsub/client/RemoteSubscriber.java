package com.george.pubsub.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RemoteSubscriber {

    private String ip;
    private int port;
    private ServerSocket serverSocket;
    private ExecutorService executor;
    private ObjectMapper mapper;
    private List<Receivable> receivers;

    public RemoteSubscriber(String ip, int port) throws IOException {
        this.ip = ip;
        this.port = port;
        mapper = new ObjectMapper();
        receivers = new ArrayList<>();
        serverSocket = new ServerSocket(port, 50, InetAddress.getByName(ip));
        System.out.println("started remote subscriber... listening on ip=" + serverSocket.getInetAddress() + " port=" + serverSocket.getLocalPort());
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
                            for (Receivable receiver : receivers) {
                                receiver.receive(message);
                            }
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

    public void addReceiver(Receivable receiver) {
        receivers.add(receiver);
    }

    public void shutdown() throws IOException {
        serverSocket.close();
        executor.shutdown();
    }

}
