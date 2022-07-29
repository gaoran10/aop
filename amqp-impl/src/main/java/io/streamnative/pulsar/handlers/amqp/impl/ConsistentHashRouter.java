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
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;

import java.util.Map;

/**
 * Direct message router.
 */
public class ConsistentHashRouter extends AbstractAmqpMessageRouter {

    public ConsistentHashRouter() {
        super(Type.Consistent_Hash);
    }

    @Override
    public boolean isMatch(Map<String, Object> properties) {
        String key = properties.get(MessageConvertUtils.PROP_ROUTING_KEY).toString();
        int queueCount = Integer.parseInt(properties.getOrDefault("__queueCount", "0").toString());
        int queueIndex = Integer.parseInt(properties.getOrDefault("__queueIndex", "0").toString());
        if (queueIndex == 0 || queueCount == 0) {
            return false;
        }
        return key.hashCode() % queueCount == queueIndex;
    }

}
