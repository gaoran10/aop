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

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpQueue;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpQueueProperties;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.IndexMessage;
import io.streamnative.pulsar.handlers.amqp.QueueContainer;
import io.streamnative.pulsar.handlers.amqp.common.exception.QueueUnavailableException;
import io.streamnative.pulsar.handlers.amqp.metcis.QueueMetrics;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Persistent queue.
 */
@Slf4j
public class PersistentQueue extends AbstractAmqpQueue {
    public static final String QUEUE = "QUEUE";
    public static final String ROUTERS = "ROUTERS";
    public static final String TOPIC_PREFIX = "__amqp_queue__";

    @Getter
    private PersistentTopic indexTopic;

    private ObjectMapper jsonMapper;

    private AmqpEntryWriter amqpEntryWriter;

    @Getter
    private QueueMetrics queueMetrics;

    private PositionImpl lastLac;
    private final Map<String, Position> exchangeLastMarkDeletePos = new ConcurrentHashMap<>();

    private final QueueContainer queueContainer;
    private volatile boolean isFenced;
    private volatile boolean isClosingOrDeleting;

    public PersistentQueue(QueueContainer queueContainer, String queueName, PersistentTopic indexTopic,
                           long connectionId,
                           boolean exclusive, boolean autoDelete,
                           QueueMetrics queueMetrics) {
        super(queueName, true, connectionId, exclusive, autoDelete);
        this.queueContainer = queueContainer;
        this.indexTopic = indexTopic;
        topicNameValidate();
        this.jsonMapper = new ObjectMapper();
        this.amqpEntryWriter = new AmqpEntryWriter(indexTopic);
        this.queueMetrics = queueMetrics;

        // TODO don't need cleanup exchange topic if write original message to queue
//        this.indexTopic.getBrokerService().getPulsar().getExecutor()
//                .schedule(this::exchangeClear, 5, TimeUnit.SECONDS);
        this.isFenced = false;
        this.isClosingOrDeleting = false;
    }

    @Override
    public CompletableFuture<Void> writeIndexMessageAsync(String exchangeName, long ledgerId, long entryId,
                                                          Map<String, Object> properties) {
        try {
            queueMetrics.writeInc();
//            Histogram.Timer writeTimer = queueMetrics.startWrite();
            IndexMessage indexMessage = IndexMessage.create(exchangeName, ledgerId, entryId, properties);
            MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(indexMessage);
            return amqpEntryWriter.publishMessage(message).whenComplete((__, t) -> {
                if (t != null) {
                    log.error("================= Failed to publish messages queue {}.", queueName);
                    queueMetrics.writeFailed();
//                    return;
                }
//                queueMetrics.writeSuccessInc();
//                queueMetrics.finishWrite(writeTimer);
            }).thenApply(__ -> null);
        } catch (Exception e) {
            log.error("Failed to writer index message for exchange {} with position {}:{}.",
                    exchangeName, ledgerId, entryId);
            queueMetrics.writeFailed();
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> writeMessageAsync(ByteBuf payload, List<KeyValue> messageKeyValues) {
        queueMetrics.writeInc();
        if (isUnavailable()) {
            queueMetrics.writeFailed();
            return FutureUtil.failedFuture(
                    new QueueUnavailableException(TopicName.get(indexTopic.getName()).getNamespace(), queueName));
        }
        return amqpEntryWriter.publishMessage(MessageConvertUtils.entryToMessage(
                payload, messageKeyValues, true))
                .whenComplete((__, t) -> {
                    if (t != null) {
                        queueMetrics.writeFailed();
                        if (FutureUtil.unwrapCompletionException(t)
                                instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                            isFenced = true;
                            close();
                        }
                    }
                })
                .thenApply(__ -> null);
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String exchangeName, long ledgerId, long entryId) {
        queueMetrics.readInc();
//        Histogram.Timer readTimer = queueMetrics.startRead();
        AmqpMessageRouter router = getRouter(exchangeName);
        if (router == null) {
            return FutureUtil.failedFuture(new RuntimeException(
                    String.format("Failed to get router between exchange %s and queue %s", exchangeName, queueName)));
        }
        return getRouter(exchangeName).getExchange().readEntryAsync(getName(), ledgerId, entryId)
                .whenComplete((__, t) -> {
            if (t != null) {
                queueMetrics.readFailed();
//                return;
            }
//            queueMetrics.finishRead(readTimer);
        });
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(String exchangeName, long ledgerId, long entryId) {
        queueMetrics.ackInc();
//        return getRouter(exchangeName).getExchange().markDeleteAsync(getName(), ledgerId, entryId);
//        PositionImpl indexMarkDeletePos = (PositionImpl) indexTopic.getManagedLedger()
//                .getSlowestConsumer().getMarkDeletedPosition();
//        Collection<CompletableFuture<Void>> futures = new ArrayList<>();
//        log.info("xxxx 1 queue ack exchange, queue: {}, exchange: {}, indexDeletePos: {}, indexLac: {}",
//                queueName, exchangeName, indexMarkDeletePos, this.lastLac);
//        if (this.lastLac == null) {
//            this.getBindExchangeLac();
//            return CompletableFuture.completedFuture(null);
//        }
//        if (indexMarkDeletePos.compareTo(this.lastLac) >= 0) {
//            for (AmqpMessageRouter router : routers.values()) {
//                Position position = exchangeLastMarkDeletePos.get(router.getExchange().getName());
//                if (position != null) {
//                    log.info("xxxx 2 queue ack exchange, queue: {}, exchange: {}, position: {}",
//                            queueName, exchangeName, position);
//                    futures.add(getRouter(exchangeName).getExchange().markDeleteAsync(
//                            queueName, position.getLedgerId(), position.getEntryId()));
//                }
//            }
//            FutureUtil.waitForAll(futures).thenRun(this::getBindExchangeLac);
//        }
//        return FutureUtil.waitForAll(futures);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> bindExchange(AmqpExchange exchange, AmqpMessageRouter router, String bindingKey,
                             Map<String, Object> arguments) {
        if (isUnavailable()) {
            return FutureUtil.failedFuture(
                    new QueueUnavailableException(TopicName.get(indexTopic.getName()).getNamespace(), queueName));
        }
        return super.bindExchange(exchange, router, bindingKey, arguments).thenApply(__ -> {
            updateQueueProperties();
            return null;
        });
    }

    @Override
    public void unbindExchange(AmqpExchange exchange) {
        if (isUnavailable()) {
            throw new QueueUnavailableException(TopicName.get(indexTopic.getName()).getNamespace(), queueName);
        }
        super.unbindExchange(exchange);
        updateQueueProperties();
    }

    @Override
    public Topic getTopic() {
        return indexTopic;
    }

    public void recoverRoutersFromQueueProperties(Map<String, String> properties,
                                                  ExchangeContainer exchangeContainer,
                                                  NamespaceName namespaceName) throws JsonProcessingException {
        if (null == properties || properties.isEmpty() || !properties.containsKey(ROUTERS)) {
            return;
        }
        List<AmqpQueueProperties> amqpQueueProperties = jsonMapper.readValue(properties.get(ROUTERS),
                new TypeReference<List<AmqpQueueProperties>>() {
                });
        if (amqpQueueProperties == null) {
            return;
        }
        amqpQueueProperties.stream().forEach((amqpQueueProperty) -> {
            // recover exchange
            String exchangeName = amqpQueueProperty.getExchangeName();
            Set<String> bindingKeys = amqpQueueProperty.getBindingKeys();
            Map<String, Object> arguments = amqpQueueProperty.getArguments();
            CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                    exchangeContainer.asyncGetExchange(namespaceName, exchangeName, false, null);
            amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to recover queue, failed to get exchange {} for queue {} in vhost {}.",
                            exchangeName, queueName, namespaceName, throwable);
                    return;
                }
                AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.
                        generateRouter(ExchangeType.value(amqpQueueProperty.getType().toString()));
                messageRouter.setQueue(this);
                messageRouter.setExchange(amqpExchange);
                messageRouter.setArguments(arguments);
                messageRouter.setBindingKeys(bindingKeys);
                amqpExchange.addQueue(this).thenAccept(__ -> routers.put(exchangeName, messageRouter));
            });
        });
    }

    private void updateQueueProperties() {
        Map<String, String> properties = new HashMap<>();
        try {
            properties.put(ROUTERS, jsonMapper.writeValueAsString(getQueueProperties(routers)));
            properties.put(QUEUE, queueName);
        } catch (JsonProcessingException e) {
            log.error("[{}] Failed to covert map of routers to String", queueName, e);
            return;
        }
        PulsarTopicMetadataUtils.updateMetaData(this.indexTopic, properties, queueName);
    }

    public static String getQueueTopicName(NamespaceName namespaceName, String queueName) {
        return TopicName.get(TopicDomain.persistent.value(),
                namespaceName, TOPIC_PREFIX + queueName).toString();
    }

    private List<AmqpQueueProperties> getQueueProperties(Map<String, AmqpMessageRouter> routers) {
        List<AmqpQueueProperties> propertiesList = new ArrayList<>();
        for (Map.Entry<String, AmqpMessageRouter> router : routers.entrySet()) {
            AmqpQueueProperties amqpQueueProperties = new AmqpQueueProperties();

            amqpQueueProperties.setExchangeName(router.getKey());
            amqpQueueProperties.setType(router.getValue().getType());
            amqpQueueProperties.setArguments(router.getValue().getArguments());
            amqpQueueProperties.setBindingKeys(router.getValue().getBindingKey());

            propertiesList.add(amqpQueueProperties);
        }
        return propertiesList;
    }

    private void topicNameValidate() {
        String[] nameArr = this.indexTopic.getName().split("/");
        checkArgument(nameArr[nameArr.length - 1].equals(TOPIC_PREFIX + queueName),
                "The queue topic name does not conform to the rules(%s%s).",
                TOPIC_PREFIX, "exchangeName");
    }

    private synchronized void exchangeClear() {
        this.lastLac = null;
        Collection<CompletableFuture<Void>> futures = new ArrayList<>();
        for (AmqpMessageRouter router : routers.values()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ((PersistentTopic) router.getExchange().getTopic()).getManagedLedger()
                    .asyncOpenCursor("__amqp_replicator__" + router.getExchange().getName(), new AsyncCallbacks.OpenCursorCallback() {
                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    PositionImpl pos = (PositionImpl) cursor.getMarkDeletedPosition();
                    exchangeLastMarkDeletePos.put(router.getExchange().getName(), pos);
                    future.complete(null);
                }

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null);
            futures.add(future);
        }
        FutureUtil.waitForAll(futures).thenAccept(__ -> {
            this.lastLac = (PositionImpl) indexTopic.getLastPosition();
            exchangeDelete();
        });
    }

    private void exchangeDelete() {
        PositionImpl indexMarkDeletePos = (PositionImpl) indexTopic.getManagedLedger()
                .getSlowestConsumer().getMarkDeletedPosition();
        Collection<CompletableFuture<Void>> futures = new ArrayList<>();
        if (indexMarkDeletePos.compareTo(this.lastLac) >= 0) {
            for (AmqpMessageRouter router : routers.values()) {
                Position position = exchangeLastMarkDeletePos.get(router.getExchange().getName());
                if (position != null) {
                    futures.add(router.getExchange().markDeleteAsync(
                            queueName, position.getLedgerId(), position.getEntryId()));
                }
            }
            FutureUtil.waitForAll(futures).thenRun(() -> {
                this.indexTopic.getBrokerService().getPulsar().getExecutor().schedule(this::exchangeClear, 5, TimeUnit.SECONDS);
            });
        } else {
            this.indexTopic.getBrokerService().getPulsar().getExecutor().schedule(this::exchangeDelete, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public boolean isUnavailable() {
        return this.isFenced || this.isClosingOrDeleting;
    }

    public CompletableFuture<Void> close() {
        log.info("Start to close queue {} in vhost {}.",
                queueName, TopicName.get(indexTopic.getName()).getNamespace());
        this.isClosingOrDeleting = true;
        if (this.indexTopic != null) {
            this.queueContainer.removeQueue(
                    TopicName.get(indexTopic.getName()).getNamespaceObject(), queueName);
            return this.indexTopic.close();
        }
        return CompletableFuture.completedFuture(null);
    }

}
