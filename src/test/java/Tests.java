import com.george.pubsub.client.PubSubClient;
import com.george.pubsub.client.RemoteSubscriber;
import com.george.pubsub.remote.RemoteAddress;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import pubsub.broker.Message;
import pubsub.broker.Receivable;

import java.io.IOException;

public class Tests {

    private PubSubClient pubSubClient;
    private RemoteSubscriber subscriber;
    private RemoteAddress localAddress;
    private final int[] messagesReceived = {0};


    public Tests() throws IOException {
        System.out.println("running constructor");
    }

    @Test
    public void testSentAndReceivedMessagesCount() throws IOException, InterruptedException {
        Thread.sleep(100);
        localAddress = new RemoteAddress("localhost", 16000);
        subscriber = new RemoteSubscriber(localAddress.getIp(), localAddress.getPort());
        pubSubClient = new PubSubClient("localhost", 15000);
        subscriber.addReceiver(new Receivable() {
            @Override
            public void receive(Message message) {

               // System.out.println(message);
                messagesReceived[0]++;
            }
        });
        pubSubClient.subscribe("test", localAddress);
        pubSubClient.subscribe("test2", localAddress);
        for (int i = 0; i < 800; i++) {
            pubSubClient.publish("test", "hello george!");
            pubSubClient.publish("test2", "test 2 hello george!");
        }
        Thread.sleep(100);
        Assertions.assertEquals(1600, messagesReceived[0], "sent and received message count must be equal");
        subscriber.shutdown();
    }

    @Test
    public void testSentAndReceivedMessagesEquals() throws IOException {
        localAddress = new RemoteAddress("localhost", 16000);
        subscriber = new RemoteSubscriber(localAddress.getIp(), localAddress.getPort());
        pubSubClient = new PubSubClient("localhost", 15000);
        subscriber.addReceiver(new Receivable() {
            @Override
            public void receive(Message message) {

                System.out.println(message);
                messagesReceived[0]++;
            }
        });
        pubSubClient.subscribe("test", localAddress);
        final Message[] receivedMessage = new Message[1];
        subscriber.addReceiver(new Receivable() {
            @Override
            public void receive(Message message) {
               receivedMessage[0] = message;
               Assertions.assertEquals("test", message.getTopic(), "sent and received message topic must be the same");
               Assertions.assertEquals("hello george!", message.getText(), "sent and received message text must be the same");
                try {
                    subscriber.shutdown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        pubSubClient.publish("test", "hello george!");
    }

}
