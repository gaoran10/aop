package io.streamnative.pulsar.handlers.amqp;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.testng.annotations.Test;

public class AdminAoPDemo extends AmqpTestBase {

//    @Test
    public void connectionTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(getAmqpBrokerPortList().get(0));
        Connection connection = factory.newConnection("test connection");
        Channel channel = connection.createChannel();
        System.out.println("start");
        Thread.sleep(1000 * 60 * 60);
    }

//    @Test
    public void bindTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(getAmqpBrokerPortList().get(0));
        factory.setVirtualHost("vhost1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String ex1_1 = "ex1_1";
        String ex1_2 = "ex1_2";
        String qu1_1 = "qu1_1";
        String qu1_2 = "qu1_2";
        String qu1_3 = "qu1_3";
        channel.exchangeDeclare(ex1_1, BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare(ex1_2, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(qu1_1, true, false, false, null);
        channel.queueDeclare(qu1_2, true, false, false, null);
        channel.queueDeclare(qu1_3, true, false, false, null);
        channel.exchangeBind(ex1_2, ex1_1, "");
        channel.queueBind(qu1_1, ex1_1, "");
        channel.queueBind(qu1_2, ex1_2, "");
        channel.queueBind(qu1_3, ex1_2, "");

        ConnectionFactory factory2 = new ConnectionFactory();
        factory2.setPort(getAmqpBrokerPortList().get(0));
        factory2.setVirtualHost("vhost2");
        Connection connection2 = factory2.newConnection();
        Channel channel2 = connection2.createChannel();

        String ex2_1 = "ex2_1";
        String ex2_2 = "ex2_2";
        String qu2_1 = "qu2_1";
        String qu2_2 = "qu2_2";
        String qu2_3 = "qu2_3";
        channel2.exchangeDeclare(ex2_1, BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare(ex2_2, BuiltinExchangeType.DIRECT, true);
        channel2.queueDeclare(qu2_1, true, false, false, null);
        channel2.queueDeclare(qu2_2, true, false, false, null);
        channel2.queueDeclare(qu2_3, true, false, false, null);
        channel2.exchangeBind(ex2_2, ex2_1, "");
        channel2.queueBind(qu2_1, ex2_1, "");
        channel2.queueBind(qu2_2, ex2_2, "");
        channel2.queueBind(qu2_3, ex2_2, "");

        Thread.sleep(1000 * 60 * 60);
    }

}
