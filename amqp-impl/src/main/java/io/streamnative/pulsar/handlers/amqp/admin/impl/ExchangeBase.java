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
package io.streamnative.pulsar.handlers.amqp.admin.impl;

import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.protocol.v0_8.FieldTable;

/**
 * Exchange base.
 */
@Slf4j
public class ExchangeBase extends BaseResources {

    protected CompletableFuture<List<ExchangeBean>> getExchangeListAsync() {
        final List<ExchangeBean> exchangeList = new ArrayList<>();
        return namespaceResource().listNamespacesAsync(tenant)
                .thenCompose(nsList -> {
                    Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
                    for (String ns : nsList) {
                        futureList.add(getExchangeListByVhostAsync(ns).thenAccept(exchangeList::addAll));
                    }
                    return FutureUtil.waitForAll(futureList);
                }).thenApply(__ -> exchangeList);
    }

    private CompletableFuture<List<String>> getExchangeListAsync(String tenant, String ns) {
        return namespaceService()
                .getFullListOfTopics(NamespaceName.get(tenant, ns))
                .exceptionally(t -> {
                    log.error("Failed to get exchange list for vhost {} in tenant {}", ns, tenant, t);
                    return new ArrayList<>();
                })
                .thenApply(list -> list.stream().filter(s ->
                        s.contains(PersistentExchange.TOPIC_PREFIX)).collect(Collectors.toList()));
    }

    protected CompletableFuture<List<ExchangeBean>> getExchangeListByVhostAsync(String vhost) {
        List<ExchangeBean> beanList = new ArrayList<>();
        return getExchangeListAsync(tenant, vhost).thenCompose(exList -> {
            Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
            exList.forEach(topic -> {
                String exchangeName = TopicName.get(topic).getLocalName()
                        .substring(PersistentExchange.TOPIC_PREFIX.length());
                log.info("get exchange bean {} in list", exchangeName);
                futureList.add(getExchangeBeanAsync(vhost, exchangeName).thenAccept(beanList::add));
            });
            return FutureUtil.waitForAll(futureList);
        }).thenApply(__ -> beanList);
    }

    protected CompletableFuture<ExchangeBean> getExchangeBeanAsync(String vhost, String exchangeName) {
        return exchangeContainer().asyncGetExchange(
                NamespaceName.get(tenant, vhost), exchangeName, false, null).thenApply(ex -> {
                    log.info("get exchange bean {}", exchangeName);
            ExchangeBean exchangeBean = new ExchangeBean();
            exchangeBean.setName(exchangeName);
            exchangeBean.setType(ex.getType().getValue());
            exchangeBean.setVhost(vhost);
            exchangeBean.setAutoDelete(ex.getAutoDelete());
            exchangeBean.setInternal(false);
            return exchangeBean;
        }).exceptionally(t -> {
            log.error("Failed to get exchange bean for exchange {} in vhost {}", exchangeName, vhost, t);
            ExchangeBean exchangeBean = new ExchangeBean();
            exchangeBean.setName(exchangeName);
            exchangeBean.setType("unknown type");
            exchangeBean.setVhost(vhost);
            exchangeBean.setAutoDelete(false);
            exchangeBean.setInternal(false);
            return exchangeBean;
        });
    }

    protected CompletableFuture<AmqpExchange> declareExchange(String vhost, String exchangeName,
                                                              ExchangeDeclareParams declareParams) {
        return exchangeService().exchangeDeclare(NamespaceName.get(tenant, vhost), exchangeName,
                declareParams.getType(), false, declareParams.isDurable(), declareParams.isAutoDelete(),
                declareParams.isInternal(), FieldTable.convertToFieldTable(declareParams.getArguments()));
    }

    protected CompletableFuture<Void> deleteExchange(String vhost, String exchangeName, boolean ifUnused) {
        return exchangeService().exchangeDelete(NamespaceName.get(tenant, vhost), exchangeName, ifUnused);
    }

}
