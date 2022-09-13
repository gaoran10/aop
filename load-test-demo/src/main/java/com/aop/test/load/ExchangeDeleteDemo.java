package com.aop.test.load;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.Cleanup;

import java.io.IOException;

public class ExchangeDeleteDemo {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        @Cleanup
        Connection connection = factory.newConnection();
        @Cleanup
        Channel channel = connection.createChannel();

        String ex = "ex";
        channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true);

        String qu = "qu";
        channel.queueDeclare(qu, true, false, true, null);
        String quKey = "qu-key";
        channel.queueBind(qu, ex, quKey);

        for (int i = 0; i < 10; i++) {
            channel.basicPublish(ex, quKey, null, "test".getBytes());
        }

//        channel.exchangeDelete(ex);
//        channel.queueDelete(qu);

        Thread.sleep(1000 * 5);

        channel.basicConsume(qu, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("receive message " + new String(body));
            }
        });

        Thread.sleep(1000 * 5);
        System.out.println("finish demo");
    }

}
