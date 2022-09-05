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
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Admin API test.
 */
@Slf4j
public class E2EDemo {


    public static void main(String[] args) throws Exception {
        E2EDemo demo = new E2EDemo();
        demo.pinTest();
    }

    public void pinTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(5682);
        @Cleanup
        Connection connection = factory.newConnection();
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
        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            keyList.add(key);
            bindAndConsume(key, Lists.newArrayList(Pair.of(predicationInput, key + ".*")));
        }

//        String authPolicyWorker = "AuthPolicyWorker";
//        channel.queueDeclare(authPolicyWorker, true, false, false, null);
//        channel.queueBind(authPolicyWorker, verificationInput, "*.*.prediction.deviceprint");
//        channel.queueBind(authPolicyWorker, analyticsInput, "*.*.prediction.fraud_risk_grouper");
//        AtomicLong receiveMsgCount1 = new AtomicLong();
//        channel.basicConsume(authPolicyWorker, true, new DefaultConsumer(channel) {
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                if (receiveMsgCount1.incrementAndGet() % 1000 == 0) {
//                    System.out.println("[" + authPolicyWorker + "] receive msg: " + new String(body));
//                }
//            }
//        });
        bindAndConsume("AuthPolicyWorker", Lists.newArrayList(
                Pair.of(verificationInput, "*.*.prediction.deviceprint"),
                Pair.of(analyticsInput, "*.*.prediction.fraud_risk_grouper")));

//        String finalizingQueue = "FinalizingQueue";
//        channel.queueDeclare(finalizingQueue, true, false, false, null);
//        channel.queueBind(finalizingQueue, finalizingInput, "");
//        AtomicLong receiveMsgCount2 = new AtomicLong();
//        channel.basicConsume(finalizingQueue, true, new DefaultConsumer(channel) {
//            @Override
//            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//                if (receiveMsgCount2.incrementAndGet() % 1000 == 0) {
//                    System.out.println("[" + finalizingQueue + "] receive msg: " + new String(body));
//                }
//            }
//        });
        bindAndConsume("FinalizingQueue", Lists.newArrayList(Pair.of(finalizingInput, "")));

        RateLimiter rateLimiter = RateLimiter.create(10000);
        long messageIndex = 0;
        while (true) {
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
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(5682);
        Connection connection = factory.newConnection();
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
