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

import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.PROP_CUSTOM_JSON;
import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.PROP_EXCHANGE;
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpBinding;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeProperties;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeReplicator;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.QueueContainer;
import io.streamnative.pulsar.handlers.amqp.common.exception.ExchangeUnavailableException;
import io.streamnative.pulsar.handlers.amqp.metcis.ExchangeMetrics;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

/**
 * Persistent Exchange.
 */
@Slf4j
public class PersistentExchange extends AbstractAmqpExchange {
    public static final String EXCHANGE = "EXCHANGE";
    public static final String QUEUES = "QUEUES";
    public static final String DES_EXCHANGES = "DES_EXCHANGES";
    public static final String BINDINGS = "BINDINGS";
    public static final String TYPE = "TYPE";
    public static final String TOPIC_PREFIX = "__amqp_exchange__";

    private PersistentTopic persistentTopic;
    private ObjectMapper jsonMapper = new JsonMapper();
    private final ConcurrentOpenHashMap<String, CompletableFuture<ManagedCursor>> cursors;
    private AmqpExchangeReplicator messageReplicator;
    private AmqpEntryWriter amqpEntryWriter;
    private ExchangeMetrics exchangeMetrics;
    private volatile boolean isFenced;
    private volatile boolean isClosingOrDeleting;
    private final ExchangeContainer exchangeContainer;
    private final QueueContainer queueContainer;

    public PersistentExchange(ExchangeContainer exchangeContainer,
                              QueueContainer queueContainer,
                              String exchangeName,
                              NamespaceName namespaceName,
                              ExchangeType type,
                              PersistentTopic persistentTopic,
                              boolean autoDelete,
                              ExchangeMetrics exchangeMetrics) {
        super(exchangeName, namespaceName, type, Sets.newConcurrentHashSet(), Sets.newConcurrentHashSet(), true, autoDelete);
        this.exchangeContainer = exchangeContainer;
        this.queueContainer = queueContainer;
        this.persistentTopic = persistentTopic;
        topicNameValidate();
        cursors = ConcurrentOpenHashMap.<String, CompletableFuture<ManagedCursor>>newBuilder()
                .expectedItems(16).concurrencyLevel(1).build();
        for (ManagedCursor cursor : persistentTopic.getManagedLedger().getCursors()) {
            cursors.put(cursor.getName(), CompletableFuture.completedFuture(cursor));
            log.info("PersistentExchange {} recover cursor {}", persistentTopic.getName(), cursor.toString());
            cursor.setInactive();
        }

        if (messageReplicator == null) {
            messageReplicator = new AmqpExchangeReplicator(this) {
                @Override
                public CompletableFuture<Void> readProcess(ByteBuf data, Position position) {
                    exchangeMetrics.routeInc();
                    Map<String, Object> props = new HashMap<>();
                    try {
                        MessageImpl<byte[]> message = MessageImpl.deserialize(data);
                        for (KeyValue keyValue : message.getMessageBuilder().getPropertiesList()) {
                            props.put(keyValue.getKey(), keyValue.getValue());
                            if (keyValue.getKey().equals(PROP_CUSTOM_JSON)) {
                                props.putAll(JsonUtil.fromJson(keyValue.getValue(), Map.class));
                            }
                        }
//                        props = message.getMessageBuilder().getPropertiesList().stream()
//                                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
//                        Histogram.Timer routeTimer = exchangeMetrics.startRoute();
                        Collection<CompletableFuture<Void>> routeFutureList = new ArrayList<>();
                        String bindingKey = props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString();
                        if (exchangeType == ExchangeType.DIRECT) {
                            Set<String> queueSet = bindingKeyQueueMap.get(bindingKey);
                            if (queueSet != null) {
                                for (String queue : queueSet) {
//                                    routeFutureList.add(
//                                            queue.writeIndexMessageAsync(exchangeName, position.getLedgerId(),
//                                                    position.getEntryId(), props));

                                    // TODO write original message
                                    CompletableFuture<Void> routeFuture =
                                            queueContainer.asyncGetQueue(namespaceName, queue, false)
                                                    .thenCompose(amqpQueue ->
                                                            amqpQueue.writeMessageAsync(
                                                                    data,
                                                                    message.getMessageBuilder().getPropertiesList()));
                                    routeFutureList.add(routeFuture);
//                                    routeFutureList.add(
//                                            queue.writeMessageAsync(data,
//                                                    message.getMessageBuilder().getPropertiesList()));
                                }
                            }
                        } else {
                            int queueIndex = 0;
                            props.put("__queueCount", queues.size());
                            for (String queue : queues) {
                                queueIndex ++;
                                props.put("__queueIndex", queueIndex);

                                // write index message
//                                CompletableFuture<Void> routeFuture = queue.getRouter(exchangeName).routingMessage(
//                                        position.getLedgerId(), position.getEntryId(),
//                                        props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
//                                        props);

                                // TODO write original message
                                CompletableFuture<Void> routeFuture =
                                        queueContainer.asyncGetQueue(namespaceName, queue, false)
                                                .thenCompose(amqpQueue ->
                                                        amqpQueue.getRouter(exchangeName).routingMessageToQueue(
                                                                data,
                                                                props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
                                                                message.getMessageBuilder().getPropertiesList(),
                                                                props));

//                                CompletableFuture<Void> routeFuture = queue.getRouter(exchangeName).routingMessageToQueue(
//                                        data,
//                                        props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
//                                        message.getMessageBuilder().getPropertiesList(),
//                                        props
//                                );
                                routeFutureList.add(routeFuture);
                            }
                        }

                        for (String exchange : exchanges) {
                            if (props.getOrDefault(PROP_EXCHANGE, "").equals(exchange)) {
                                // indicate this message is from the destination exchange
                                // don't need to route, avoid dead loop
                                continue;
                            }
                            props.put(PROP_EXCHANGE, exchange);
                            CompletableFuture<Void> routeFuture =
                                    exchangeContainer.asyncGetExchange(namespaceName, exchange, false, null, false)
                                            .thenCompose(amqpExchange -> amqpExchange.getRouter(exchangeName).routingMessageToEx(
                                                    data,
                                                    props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
                                                    message.getMessageBuilder().getPropertiesList(),
                                                    props));
                            routeFutureList.add(routeFuture);
                        }

                        return FutureUtil.waitForAll(routeFutureList).whenComplete((__, t) -> {
                            if (t != null) {
                                exchangeMetrics.routeFailedInc();
//                                return;
                            }
//                            exchangeMetrics.finishRoute(routeTimer);
                        });
                    } catch (Exception e) {
                        log.error("================= Read process failed. exchangeName: {}", exchangeName, e);
                        return FutureUtil.failedFuture(e);
                    }
                }
            };
            messageReplicator.startReplicate();
        }
        this.amqpEntryWriter = new AmqpEntryWriter(persistentTopic);
        this.exchangeMetrics = exchangeMetrics;
    }

    @Override
    public CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey) {
        exchangeMetrics.writeInc();
//        Histogram.Timer writeTimer = exchangeMetrics.startWrite();
        return amqpEntryWriter.publishMessage(message).whenComplete((__, t) -> {
            if (t != null) {
                exchangeMetrics.writeFailed();
                if (FutureUtil.unwrapCompletionException(t)
                        instanceof ManagedLedgerException.ManagedLedgerFencedException) {
                    isFenced = true;
                    close();
                }
                return;
            }
//            exchangeMetrics.writeSuccessInc();
//            exchangeMetrics.finishWrite(writeTimer);
        });
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, long ledgerId, long entryId) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        return readEntryAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, Position position) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        exchangeMetrics.readInc();
        CompletableFuture<Entry> future = new CompletableFuture<>();
        // TODO Temporarily put the creation operation here, and later put the operation in router
        CompletableFuture<ManagedCursor> cursorFuture = cursors.get(queueName);
        cursorFuture.thenAccept(cursor -> {
            if (cursor == null) {
                future.completeExceptionally(new ManagedLedgerException("cursor is null"));
                return;
            }
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();
//            Histogram.Timer readTimer = exchangeMetrics.startRead();
            ledger.asyncReadEntry((PositionImpl) position, new AsyncCallbacks.ReadEntryCallback() {
                        @Override
                        public void readEntryComplete(Entry entry, Object o) {
//                            exchangeMetrics.finishRead(readTimer);
                            future.complete(entry);
                        }

                        @Override
                        public void readEntryFailed(ManagedLedgerException e, Object o) {
                            exchangeMetrics.readFailed();
                            future.completeExceptionally(e);
                        }
                    }
                    , null);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, long ledgerId, long entryId) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        return markDeleteAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, Position position) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        exchangeMetrics.ackInc();
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture<ManagedCursor> cursorFuture = cursors.get(queueName);
        cursorFuture.thenAccept(cursor -> {
            if (cursor == null) {
                future.completeExceptionally(new RuntimeException(
                        "The cursor " + queueName + " of the exchange " + exchangeName + " is null."));
                return;
            }
            if (((PositionImpl) position).compareTo((PositionImpl) cursor.getMarkDeletedPosition()) < 0) {
                future.complete(null);
                return;
            }
            cursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Mark delete success for position: {}", exchangeName, position);
                    }
                    future.complete(null);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException e, Object ctx) {
                    if (((PositionImpl) position).compareTo((PositionImpl) cursor.getMarkDeletedPosition()) < 0) {
                        log.warn("Mark delete failed for position: {}, {}", position, e.getMessage());
                    } else {
                        log.error("Mark delete failed for position: {}", position, e);
                    }
                    future.completeExceptionally(e);
                }
            }, null);
        });
        return future;
    }

    @Override
    public CompletableFuture<Position> getMarkDeleteAsync(String queueName) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        CompletableFuture<ManagedCursor> cursorFuture = cursors.get(queueName);
        return cursorFuture.thenApply(cursor -> {
            if (cursor == null) {
                throw new RuntimeException("The cursor " + queueName + " is null.");
            }
            return cursor.getMarkDeletedPosition();
        });
    }


    @Override
    public CompletableFuture<Void> addQueue(AmqpQueue queue) {
        if (this.isUnavailable()) {
            throw new ExchangeUnavailableException(
                    TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName);
        }
        queues.add(queue.getName());
        if (exchangeType == ExchangeType.DIRECT) {
            for (String bindingKey : queue.getRouter(exchangeName).getBindingKey()) {
                bindingKeyQueueMap.compute(bindingKey, (k, v) -> {
                    if (v == null) {
                        Set<String> set = new HashSet<>();
                        set.add(queue.getName());
                        return set;
                    } else {
                        v.add(queue.getName());
                        return v;
                    }
                });
            }
        }
        updateExchangeProperties();

        // TODO write original data to queue, don't need to create the cursor
//        return createCursorIfNotExists(queue.getName()).thenApply(__ -> null);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void removeQueue(AmqpQueue queue) {
        if (this.isUnavailable()) {
            throw new ExchangeUnavailableException(
                    TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName);
        }
        queues.remove(queue.getName());
        if (bindingKeyQueueMap != null) {
            for (Set<String> queueSet : bindingKeyQueueMap.values()) {
                queueSet.remove(queue.getName());
            }
        }
        updateExchangeProperties();
//        deleteCursor(queue.getName());
    }

    @Override
    public CompletableFuture<Void> bindExchange(AmqpExchange sourceEx, String routingKey, Map<String, Object> params) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        routerMap.compute(sourceEx.getName(), (k, router) -> {
            if (router == null) {
                router = AbstractAmqpMessageRouter.generateRouter(sourceEx.getType());
            }
            AmqpBinding binding = new AmqpBinding(sourceEx.getName(), routingKey, params);
            router.addBinding(binding);
            router.setExchange(sourceEx);
            router.setDestinationExchange(this);
            router.setArguments(params);
            return router;
        });
        updateExchangeProperties();
        return sourceEx.addExchange(this, routingKey, params);
    }

    @Override
    public CompletableFuture<Void> unbindExchange(AmqpExchange sourceEx, String routingKey, Map<String, Object> params) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        routerMap.computeIfPresent(sourceEx.getName(), (k, router) -> {
            router.getBindings().remove(new AmqpBinding(sourceEx.getName(), routingKey, params).getPropsKey());
            return router;
        });
        updateExchangeProperties();
        sourceEx.removeExchange(this, routingKey, params);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addExchange(AmqpExchange destinationEx, String routingKey, Map<String, Object> params) {
        if (this.isUnavailable()) {
            return FutureUtil.failedFuture(
                    new ExchangeUnavailableException(
                            TopicName.get(this.persistentTopic.getName()).getNamespace(), exchangeName));
        }
        exchanges.add(destinationEx.getName());
        if (exchangeType == ExchangeType.DIRECT) {
            bindingKeyExchangeMap.compute(routingKey, (k, v) -> {
                if (v == null) {
                    v = Sets.newConcurrentHashSet();
                }
                v.add(destinationEx);
                return v;
            });
        }

        updateExchangeProperties();

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void removeExchange(AmqpExchange destinationEx, String routingKey, Map<String, Object> params) {
        if (exchangeType == ExchangeType.DIRECT) {
            bindingKeyExchangeMap.computeIfPresent(routingKey, (k, v) -> {
                v.remove(destinationEx);
                return v;
            });
        }
        if (routerMap.get(destinationEx.getName()) == null
                || routerMap.get(destinationEx.getName()).getBindings().isEmpty()) {
            exchanges.remove(destinationEx);
//            deleteCursor(getExBindCursorName(destinationEx));
        }
    }

    private String getExBindCursorName(AmqpExchange exchange) {
        return "__des_ex_" + exchange.getName();
    }

    @Override
    public int getExchangeSize() {
        return exchanges.size();
    }

    @Override
    public AmqpMessageRouter getRouter(String sourceEx) {
        return routerMap.get(sourceEx);
    }

    @Override
    public Map<String, AmqpMessageRouter> getRouters() {
        return routerMap;
    }

    @Override
    public Topic getTopic(){
        return persistentTopic;
    }

    private void updateExchangeProperties() {
        Map<String, String> properties = new HashMap<>();
        try {
            properties.put(EXCHANGE, exchangeName);
            properties.put(TYPE, exchangeType.getValue());
            List<String> queueNames = getQueueNames();
            if (queueNames.size() != 0) {
                properties.put(QUEUES, jsonMapper.writeValueAsString(getQueueNames()));
            }
            properties.put(DES_EXCHANGES, jsonMapper.writeValueAsString(getExchangeNames()));
            properties.put(BINDINGS, jsonMapper.writeValueAsString(getBindingsData()));
        } catch (JsonProcessingException e) {
            log.error("[{}] covert map of routers to String error: {}", exchangeName, e.getMessage());
            return;
        }
        PulsarTopicMetadataUtils.updateMetaData(this.persistentTopic, properties, exchangeName);
    }

    private List<String> getQueueNames() {
//        List<String> queueNames = new ArrayList<>();
//        for (AmqpQueue queue : queues) {
//            queueNames.add(queue.getName());
//        }
//        return queueNames;
        return new ArrayList<>(queues);
    }

    private List<String> getExchangeNames() {
        return new ArrayList<>(exchanges);
    }

    private List<AmqpExchangeProperties> getBindingsData() {
        List<AmqpExchangeProperties> propertiesList = new ArrayList<>(routerMap.size());
        for (AmqpMessageRouter router : routerMap.values()) {
            AmqpExchangeProperties properties = new AmqpExchangeProperties();
            properties.setExchangeName(router.getExchange().getName());
            properties.setType(router.getExchange().getType());
            properties.setBindings(router.getBindings());
            properties.setArguments(router.getArguments());
            propertiesList.add(properties);
        }
        return propertiesList;
    }

    public void recover(Map<String, String> properties, ExchangeContainer exchangeContainer,
                        NamespaceName namespaceName) throws JsonProcessingException {
        if (null == properties || properties.isEmpty() || !properties.containsKey(BINDINGS)) {
            return;
        }
        List<AmqpExchangeProperties> amqpExchangeProperties = jsonMapper.readValue(properties.get(BINDINGS),
                new TypeReference<List<AmqpExchangeProperties>>() {});
        if (amqpExchangeProperties == null) {
            return;
        }
        for (AmqpExchangeProperties props : amqpExchangeProperties) {
            exchangeContainer.asyncGetExchange(namespaceName, props.getExchangeName(), false, null)
                    .thenAccept(ex -> {
                        AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.generateRouter(props.getType());
                        messageRouter.setExchange(ex);
                        messageRouter.setDestinationExchange(this);
                        messageRouter.setBindings(props.getBindings());
                        messageRouter.setArguments(props.getArguments());
                    });
        }
    }

    private CompletableFuture<ManagedCursor> createCursorIfNotExists(String name) {
        CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
        return cursors.computeIfAbsent(name, cusrsor -> {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            if (log.isDebugEnabled()) {
                log.debug("Create cursor {} for topic {}", name, persistentTopic.getName());
            }
            ledger.asyncOpenCursor(name, CommandSubscribe.InitialPosition.Earliest, new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        cursorFuture.complete(cursor);
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("[{}] Failed to open cursor. ", name, exception);
                        cursorFuture.completeExceptionally(exception);
                        if (cursors.get(name) != null && cursors.get(name).isCompletedExceptionally()
                                || cursors.get(name).isCancelled()) {
                            cursors.remove(name);
                        }
                    }
                }, null);
            return cursorFuture;
        });
    }

    public void deleteCursor(String name) {
        CompletableFuture<ManagedCursor> cursorFuture = cursors.remove(name);
        if (cursorFuture == null) {
            log.warn("The queue {} bind to exchange {} is not exist.", name, exchangeName);
            return;
        }
        cursorFuture.thenAccept(cursor -> {
            if (cursor != null) {
                persistentTopic.getManagedLedger().asyncDeleteCursor(cursor.getName(),
                        new AsyncCallbacks.DeleteCursorCallback() {
                            @Override
                            public void deleteCursorComplete(Object ctx) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Cursor {} for topic {} deleted successfully .",
                                            cursor.getName(), persistentTopic.getName());
                                }
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                log.error("[{}] Error deleting cursor for topic {}.",
                                        cursor.getName(), persistentTopic.getName(), exception);
                            }
                        }, null);
            }
        });
    }

    public static String getExchangeTopicName(NamespaceName namespaceName, String exchangeName) {
        return TopicName.get(TopicDomain.persistent.value(),
                namespaceName, TOPIC_PREFIX + exchangeName).toString();
    }

    public void topicNameValidate() {
        String[] nameArr = this.persistentTopic.getName().split("/");
        checkArgument(nameArr[nameArr.length - 1].equals(TOPIC_PREFIX + exchangeName),
                "The exchange topic name does not conform to the rules(__amqp_exchange__exchangeName).");
    }

    @Override
    public boolean isUnavailable() {
        return isFenced && this.isClosingOrDeleting;
    }

    public CompletableFuture<Void> close() {
        log.info("Start to close exchange {} in vhost {}.",
                exchangeName, TopicName.get(persistentTopic.getName()).getNamespace());
        this.isClosingOrDeleting = true;
        if (this.messageReplicator != null) {
            this.messageReplicator.stopReplicate();
        }
        if (this.persistentTopic != null) {
            this.exchangeContainer.removeExchange(
                    TopicName.get(persistentTopic.getName()).getNamespaceObject(), exchangeName);
            return this.persistentTopic.close();
        }
        return CompletableFuture.completedFuture(null);
    }

}
