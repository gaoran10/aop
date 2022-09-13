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
package com.aop.test.load;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Admin API test.
 */
@Slf4j
public class ExchangeBindExchangeDemo {

    @Parameter(names = "--host", description = "AMQP server host name")
    private String host = "localhost";

    @Parameter(names = "--port", description = "AMQP server port")
    private Integer port = 5672;

    @Parameter(names = "--virtual-host", description = "AMQP virtual host")
    private String virtualHost;

    @Parameter(names = "--queue-count", description = "Bind queue count")
    private Integer queueCount = 10;

    @Parameter(names = "--message-total-count", description = "Message total count")
    private long messageTotalCount = -1L;

    @Parameter(names = "--publish-rate", description = "Message publish rate")
    private Integer publishRate = 1000;

    @Parameter(names = "-debug", description = "Debug mode")
    private boolean debug = false;

    private long messagePublishedCount;

    private long messageConsumedCount;

    public static void main(String[] args) throws Exception {
        ExchangeBindExchangeDemo demo = new ExchangeBindExchangeDemo();
        JCommander.newBuilder()
                .addObject(demo)
                .build()
                .parse(args);
        demo.pinTest();
    }

    private Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        if (StringUtils.isBlank(virtualHost)) {
            factory.setVirtualHost(virtualHost);
        }
        return factory.newConnection();
    }

    public void pinTest() throws Exception {
        @Cleanup
        Connection connection = getConnection();
        @Cleanup
        Channel channel = connection.createChannel();
        String predicationInput, predicationInputHeaders, verificationInput, analyticsInput, finalizingInput;
        predicationInput = "prediction-input";
        predicationInputHeaders = "prediction-input.headers";
        verificationInput = "verification-input";
        analyticsInput = "analytics-input";
        finalizingInput = "finalizing-input";
        channel.exchangeDeclare(predicationInput, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(predicationInputHeaders, BuiltinExchangeType.HEADERS, true);
        channel.exchangeDeclare(verificationInput, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(analyticsInput, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(finalizingInput, BuiltinExchangeType.TOPIC, true);

        channel.exchangeBind(predicationInputHeaders, predicationInput, "#");

        Map<String, Object> map1 = new HashMap<>();
        map1.put("verification-requested", true);
        channel.exchangeBind(verificationInput, predicationInputHeaders, "", map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("analytics-requested", true);
        channel.exchangeBind(analyticsInput, predicationInputHeaders, "", map2);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("finalizing", true);
        channel.exchangeBind(finalizingInput, predicationInputHeaders, "", map3);

        List<String> keyList = new ArrayList<>();
        keyList.add("a.b.prediction.deviceprint");
        keyList.add("x.y.prediction.fraud_risk_grouper");
        keyList.add("FinalizingQueue");
        for (int i = 0; i < queueCount; i++) {
            String key = "key-" + i;
            keyList.add(key);
            bindAndConsume(key, Lists.newArrayList(Pair.of(predicationInput, key + ".*")));
        }

        bindAndConsume("AuthPolicyWorker", Lists.newArrayList(
                Pair.of(verificationInput, "*.*.prediction.deviceprint"),
                Pair.of(analyticsInput, "*.*.prediction.fraud_risk_grouper")));

        bindAndConsume("FinalizingQueue", Lists.newArrayList(Pair.of(finalizingInput, "")));

        RateLimiter rateLimiter = RateLimiter.create(publishRate);
        long messageIndex = 0;
        while (messageIndex < messageTotalCount || messageTotalCount == -1) {
            rateLimiter.acquire();
            int index = (int) (messageIndex % keyList.size());
            String key;
            if (index == 0) {
                AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
                Map<String, Object> headers = new HashMap<>();
                headers.put("verification-requested", true);
                basicProperties.headers(headers);
                key = "a.b.prediction.deviceprint";
                String msg = "[" + messageIndex + "] with key " + key;
                channel.basicPublish(predicationInput, key, basicProperties.build(), msg.getBytes());
            } else if (index == 1) {
                AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
                Map<String, Object> headers = new HashMap<>();
                headers.put("analytics-requested", true);
                basicProperties.headers(headers);
                key = "x.y.prediction.fraud_risk_grouper";
                String msg = "[" + messageIndex + "] with key " + key;
                channel.basicPublish(predicationInput, key, basicProperties.build(), msg.getBytes());
            } else if (index == 2) {
                AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
                Map<String, Object> headers = new HashMap<>();
                headers.put("finalizing", true);
                basicProperties.headers(headers);
                key = "FinalizingQueue";
                String msg = "[" + messageIndex + "] with key " + key;
                channel.basicPublish(predicationInput, key, basicProperties.build(), msg.getBytes());
            } else {
                key = keyList.get(index) + ".a";
                String msg = "[" + messageIndex + "] with key " + key;
                channel.basicPublish(predicationInput, key, null, msg.getBytes());
            }
            messageIndex++;
            if (messageIndex % 1000 == 0) {
                System.out.println("total send messages " + messageIndex);
            }
        }
    }

    private void bindAndConsume(String queue, List<Pair<String, String>> bindings) throws Exception {
        Connection connection = getConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(queue, true, false, true, null);
        for (Pair<String, String> binding : bindings) {
            channel.queueBind(queue, binding.getKey(), binding.getValue());
        }

        AtomicLong receiveMsg = new AtomicLong();
        channel.basicConsume(queue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                channel.basicAck(envelope.getDeliveryTag(), false);
                if (receiveMsg.incrementAndGet() == 1) {
                    System.out.println("[" + queue + "] receive msg");
                }
            }
        });
        System.out.println("bind and consume " + queue);
    }

}
