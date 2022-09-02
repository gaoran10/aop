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
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Admin API test.
 */
@Slf4j
public class TimoutTest extends AmqpTestBase{

    @Test()
    public void falistExchangeTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(getProxyPort());
        factory.setVirtualHost("vhost1");
        factory.setConnectionTimeout(10000);
        Connection connection = factory.newConnection();
        System.out.println("start sleep");
        Thread.sleep(1000 * 30);
        System.out.println("finish sleep");
        Channel channel = connection.createChannel();
        channel.close();
        connection.close();
    }

}
