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
package io.streamnative.pulsar.handlers.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Admin API test.
 */
@Slf4j
public class AutoDeleteTest extends AmqpTestBase{

    @Test()
    public void test() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();
        Connection connection2 = getConnection("vhost1", true);
        Channel channel2 = connection2.createChannel();

        String ex = randExName();
        String qu = randQuName();

        channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true, false, null);
        channel.exchangeDeclarePassive(ex);

        channel.queueDeclare(qu, true, true, true, null);
        channel.queueDeclarePassive(qu);

        channel.basicConsume(qu, true, new DefaultConsumer(channel));
        channel.close();

        connection = getConnection("vhost1", true);
        channel = connection.createChannel();
        try {
            channel.queueDeclarePassive(qu);
            Assert.fail("Should fail declare passive no exist queue");
        } catch (Exception e) {
            // expected
        }

        System.out.println("connection isOpen: " + connection.isOpen());
        connection = getConnection("vhost1", true);
        channel = connection.createChannel();
        System.out.println("declare queue again");
        channel.queueDeclare(qu, true, true, true, null);

        channel.exchangeDeclarePassive(ex);

        channel.queueBind(qu, ex, "");
        channel.queueUnbind(qu, ex, "");
//        try {
//            channel.exchangeDeclarePassive(ex);
//            Assert.fail("Should fail declare passive no exist exchange");
//        } catch (Exception e) {
//            // expected
//        }

        connection.close();
    }

    @Test()
    public void queueAutoDeleteTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();

        String qu = randQuName();

        channel.queueDeclare(qu, true, true, true, null);
        channel.queueDeclarePassive(qu);

        Set<String> messageSet = new HashSet<>();
        channel.basicConsume(qu, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("receive message ");
                messageSet.remove(new String(body));
            }
        });

        messageSet.add("test");
        channel.basicPublish("", qu, null, "test".getBytes());

        Awaitility.await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.SECONDS)
                .until(messageSet::isEmpty);
        channel.close();
        connection.close();

        System.out.println("connection isOpen: " + connection.isOpen());
        connection = getConnection("vhost1", true);
        channel = connection.createChannel();
        System.out.println("declare queue again");

        channel.queueDeclare(qu, true, true, true, null);

        Set<String> messageSet2 = new HashSet<>();
        channel.basicConsume(qu, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("receive message 2");
                messageSet2.remove(new String(body));
            }
        });

        messageSet2.add("test");
        channel.basicPublish("", qu, null, "test".getBytes());

        Awaitility.await().pollInterval(1, TimeUnit.SECONDS)
                .atMost(2, TimeUnit.SECONDS)
                .until(messageSet2::isEmpty);

        connection.close();
    }

    @Test
    public void exclusiveTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();

        String qu = randQuName();
        channel.queueDeclare(qu, true, true, false, null);

        Connection connection2 = getConnection("vhost1", true);
        Channel channel2 = connection2.createChannel();

        try {
            channel2.queueDeclare(qu, true, true, false, null);
        } catch (Exception e) {
            // expected exception
        }
        assertFalse(channel2.isOpen());
        assertTrue(connection2.isOpen());
    }

}
