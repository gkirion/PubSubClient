import com.george.pubsub.client.PubSubClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import java.io.IOException;

public class Tests {

    private PubSubClient pubSubClient;
    private final int[] messagesReceived = {0};


    public Tests() throws IOException {
        System.out.println("running constructor");
    }

    @Test
    public void testSentAndReceivedMessagesCount() throws IOException, InterruptedException {
        Thread.sleep(200);
        pubSubClient = new PubSubClient("localhost", 16000);
        pubSubClient.setRemoteBrokerIp("localhost");
        pubSubClient.setRemoteBrokerPort(15000);
        Receivable receivable = new Receivable() {
            @Override
            public void receive(Message message) {

                // System.out.println(message);
                messagesReceived[0]++;
            }
        };
        pubSubClient.subscribe("test", receivable);
        pubSubClient.subscribe("test2", receivable);
        for (int i = 0; i < 800; i++) {
            pubSubClient.publish("test", "hello george!");
            pubSubClient.publish("test2", "test 2 hello george!");
        }
        Thread.sleep(100);
        Assertions.assertEquals(1600, messagesReceived[0], "sent and received message count must be equal");
        pubSubClient.shutdown();
    }

    @Test
    public void testSentAndReceivedMessagesEquals() throws IOException {
        pubSubClient = new PubSubClient("localhost", 16000);
        pubSubClient.setRemoteBrokerIp("localhost");
        pubSubClient.setRemoteBrokerPort(15000);

        pubSubClient.subscribe("test", new Receivable() {
            @Override
            public void receive(Message message) {
               Assertions.assertEquals("test", message.getTopic(), "sent and received message topic must be the same");
               Assertions.assertEquals("hello george!", message.getText(), "sent and received message text must be the same");
                try {
                    pubSubClient.shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        pubSubClient.publish("test", "hello george!");
    }

}
