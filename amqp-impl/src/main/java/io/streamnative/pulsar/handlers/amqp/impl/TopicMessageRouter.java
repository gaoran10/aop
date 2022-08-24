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
package io.streamnative.pulsar.handlers.amqp.impl;

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpBinding;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.Map;
import org.apache.qpid.server.exchange.topic.TopicParser;

/**
 * Topic message router.
 */
public class TopicMessageRouter extends AbstractAmqpMessageRouter {

    public TopicMessageRouter() {
        super(Type.Topic);
    }

    private TopicParser parser = new TopicParser();

    /**
     * Use Qpid.
     *
     * @param routingKey
     * @return
     */
    public boolean isMatch(String routingKey) {
        updateParserIfNeeded();
        return parser.parse(routingKey).size() > 0;
    }

    /**
     * Use Qpid.
     *
     * @param properties
     * @return
     */
    @Override
    public boolean isMatch(Map<String, Object> properties) {
        String routingKey = properties.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString();
        return isMatch(routingKey);
    }

    private void updateParserIfNeeded() {
        if (!haveChanges) {
            return;
        }
        parser = new TopicParser();
        for (String bindingKey : this.bindingKeys) {
            parser.addBinding(bindingKey, null);
        }
        for (AmqpBinding binding : this.bindings.values()) {
            parser.addBinding(binding.getBindingKey(), null);
        }
        this.haveChanges = false;
    }

}
