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
package io.streamnative.pulsar.handlers.amqp.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.utils.HttpUtil;
import io.streamnative.pulsar.handlers.amqp.utils.JsonUtil;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Amqp admin.
 */
public class AmqpAdmin {

    private final String baseUrl;

    public AmqpAdmin(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port + "/api";
    }

    public CompletableFuture<Void> exchangeDeclare(NamespaceName namespaceName,
                                                   String exchange,
                                                   ExchangeDeclareParams exchangeDeclareParams) {
        String url = String.format("%s/exchanges/%s/%s", baseUrl, namespaceName.getLocalName(), exchange);
        try {
            return HttpUtil.putAsync(url, JsonUtil.toMap(exchangeDeclareParams), Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return  CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueDeclare(NamespaceName namespaceName,
                                                String queue,
                                                QueueDeclareParams queueDeclareParams) {
        String url = String.format("%s/queues/%s/%s", baseUrl, namespaceName.getLocalName(), queue);
        try {
            return HttpUtil.putAsync(url, JsonUtil.toMap(queueDeclareParams), Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueBind(NamespaceName namespaceName,
                                             String exchange,
                                             String queue,
                                             BindingParams bindingParams) {
        String url = String.format("%s/bindings/%s/e/%s/q/%s", baseUrl, namespaceName.getLocalName(), exchange, queue);
        try {
            return HttpUtil.postAsync(url, JsonUtil.toMap(bindingParams), Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueUnbind(NamespaceName namespaceName,
                                               String exchange,
                                               String queue,
                                               String props) {
        String url = String.format("%s/bindings/%s/e/%s/q/%s/%s",
                baseUrl, namespaceName.getLocalName(), exchange, queue, props);
        return HttpUtil.deleteAsync(url, null, Map.of("tenant", namespaceName.getTenant()));
    }

}
