/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.rabbitmq.authentication;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import io.streamnative.pulsar.handlers.amqp.AmqpTokenAuthenticationTestBase;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Cleanup;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * PlainAuthenticationTest tests the plain authentication.
 */
public class PlainAuthenticationTest extends AmqpTokenAuthenticationTestBase {
    private void testConnect(int port) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("superUser2");
        connectionFactory.setPassword("superpassword");
        @Cleanup
        Connection connection = connectionFactory.newConnection();
        @Cleanup
        Channel ignored = connection.createChannel();
    }

    private Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5682);
        connectionFactory.setUsername("superUser2");
        connectionFactory.setPassword("superpassword2");
        Connection connection = connectionFactory.newConnection();
        return connection;
    }

    protected void basicDirectConsume() throws Exception {
        String exchangeName = randExName();
        String routingKey = "test.key";
        String queueName = randQuName();

        Connection conn = getConnection();
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        int messageCnt = 100;
        CountDownLatch countDownLatch = new CountDownLatch(messageCnt);

        AtomicInteger consumeIndex = new AtomicInteger(0);
        channel.basicConsume(queueName, true, "", false, true, null,
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) throws IOException {
                        long deliveryTag = envelope.getDeliveryTag();
//                        Assert.assertEquals(new String(body), "Hello, world! - " + consumeIndex.getAndIncrement());
                        System.out.println("receive msg: " + new String(body));
                        consumeIndex.incrementAndGet();
                        // (process the message components here ...)
//                        channel.basicAck(deliveryTag, false);
                        countDownLatch.countDown();
                    }
                });

        for (int i = 0; i < messageCnt; i++) {
            byte[] messageBodyBytes = ("Hello, world! - " + i).getBytes();
            channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }

        countDownLatch.await();
        Assert.assertEquals(messageCnt, consumeIndex.get());
        channel.close();
        conn.close();
    }

    @Test
    public void testConnectToBroker() throws Exception {
        basicDirectConsume();
    }

    @Test
    public void testConnectToProxy() throws Exception {
        testConnect(getAopProxyPortList().get(0));
    }

    private void testConnectWithInvalidToken(int port, boolean isProxy) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost("vhost1");
        connectionFactory.setUsername("superUser2");
        connectionFactory.setPassword("invalidpassword");

        Exception exception;
        if (isProxy) {
            exception = expectThrows(IOException.class,
                    connectionFactory::newConnection);
        } else {
            exception = expectThrows(PossibleAuthenticationFailureException.class,
                    connectionFactory::newConnection);
        }
        assertTrue(exception.getCause().getMessage().contains("Authentication failed"));
    }

    @Test
    public void testConnectToBrokerWithInvalidToken() throws IOException, TimeoutException {
        testConnectWithInvalidToken(getAmqpBrokerPortList().get(0), false);
    }

    @Test
    public void testConnectToProxyWithInvalidToken() throws IOException, TimeoutException {
        testConnectWithInvalidToken(getAopProxyPortList().get(0), true);
    }
}
