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

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.CommandSubscribe;

/**
 * Amqp exchange replicator, read entries from BookKeeper and process entries.
 */
@Slf4j
public abstract class AmqpExchangeReplicator implements AsyncCallbacks.ReadEntriesCallback,
        AsyncCallbacks.DeleteCallback {

    private PersistentExchange persistentExchange;
    private final String cursorNamePre = "__amqp_replicator__";
    private String name;
    private PersistentTopic topic;
    private ManagedCursor cursor;
    private ScheduledExecutorService scheduledExecutorService;

    protected final Backoff backOff = new Backoff(
            100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private static final AtomicReferenceFieldUpdater<AmqpExchangeReplicator, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(AmqpExchangeReplicator.class, State.class, "state");
    private volatile State state  = State.Stopped;
    /**
     * Replicator state.
     */
    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    private static final int defaultReadMaxSizeBytes = 4 * 1024 * 1024;
    private static final int replicatorQueueSize = 2000;
    private volatile int pendingQueueSize = 0;
    private static final AtomicIntegerFieldUpdater<AmqpExchangeReplicator> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReplicator.class, "pendingQueueSize");

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<AmqpExchangeReplicator> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReplicator.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    private final Backoff readFailureBackoff = new Backoff(
            1, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private final ScheduledExecutorService markDeleteExecutor = Executors.newSingleThreadScheduledExecutor(
            new DefaultThreadFactory("router-mark-delete"));
    private final ConcurrentLinkedDeque<Position> markDeletePositionDeque = new ConcurrentLinkedDeque<>();
    private final Executor executorService = Executors.newSingleThreadExecutor(
            new DefaultThreadFactory("amqp-ex-router"));

    protected AmqpExchangeReplicator(PersistentExchange persistentExchange) {
        this.persistentExchange = persistentExchange;
        this.topic = (PersistentTopic) persistentExchange.getTopic();
        this.scheduledExecutorService = topic.getBrokerService().executor();
        STATE_UPDATER.set(this, AmqpExchangeReplicator.State.Stopped);
        this.name = "[AMQP Replicator for " + topic.getName() + " ]";
        markDeleteExecutor.scheduleAtFixedRate(
                this::markDeletePositions, 100, 100, TimeUnit.MILLISECONDS);
    }

    public void startReplicate() {
        if (STATE_UPDATER.get(AmqpExchangeReplicator.this).equals(AmqpExchangeReplicator.State.Stopping)) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Waiting for producer close before attempting reconnect, retrying in {} s",
                        name, waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state.equals(State.Started)) {
                // already running
                if (log.isDebugEnabled()) {
                    log.debug("{} Replicator was already running.", name);
                }
            } else {
                log.debug("{} Replicator was already started. Replicator State: {}", name, state);
            }
            return;
        }

        log.info("{} Replicator is starting.", name);

        topic.getManagedLedger().asyncOpenCursor(cursorNamePre + persistentExchange.getName(),
                CommandSubscribe.InitialPosition.Earliest,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor managedCursor, Object o) {
                        log.info("{} Open cursor succeed for route.", name);
                        AmqpExchangeReplicator.this.cursor = managedCursor;
                        readEntries();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException e, Object o) {
                        retryStartReplicator(e);
                    }
                }, null);
    }

    private void retryStartReplicator(Throwable ex) {
        if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Failed to start replicator, errorMsg: {}, retrying in {} s.",
                        name, ex.getMessage(), waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
        } else {
            log.error("{} Failed to start replicator, errorMsg: {}", name, ex.getMessage(), ex);
        }
    }

    private void readEntries() {
        // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
        cursor.rewind();
        cursor.cancelPendingReadRequest();

        backOff.reset();
        // activate cursor: so, entries can be cached
        cursor.setActive();

        STATE_UPDATER.set(this, State.Started);
        log.info("{} Replicator is started.", name);

        readMoreEntries();
    }

    private void readMoreEntries() {
        log.info("xxxx {} Read more entries.", name);
        if (log.isDebugEnabled()) {
            log.debug("{} Read more entries.", name);
        }
        int availablePermits = getAvailablePermits();
        if (availablePermits > 0) {
            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Schedule read of {} messages.", name, availablePermits);
                }
                cursor.asyncReadEntriesOrWait(availablePermits, defaultReadMaxSizeBytes, this, null, null);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} Not schedule read due to pending read. Messages to read {}.",
                            name, availablePermits);
                }
            }
        } else {
            // no permits from rate limit
            scheduledExecutorService.schedule(this::readMoreEntries, 1, TimeUnit.MILLISECONDS);
        }
    }

    private int getAvailablePermits() {
        int availablePermits = replicatorQueueSize - PENDING_SIZE_UPDATER.get(this);
        if (availablePermits <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("{} Replicator queue is full, availablePermits: {}, pause route.",
                        name, availablePermits);
            }
            log.info("xxxx {} Replicator queue is full, availablePermits: {}, pause route.",
                    name, availablePermits);
            return 0;
        }
        log.info("xxxx getAvailablePermits {}", availablePermits);
        return availablePermits;
    }

    @Override
    public void readEntriesComplete(List<Entry> list, Object o) {
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries complete. Entries size: {}", name, list.size());
        }
        readFailureBackoff.reset();
        List<Pair<PositionImpl, ByteBuf>> bufList = new ArrayList<>(list.size());
        for (Entry entry : list) {
            entry.getDataBuffer().retain();
            bufList.add(
                    Pair.of(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()), entry.getDataBuffer()));
        }
        this.readComplete(bufList);
//        executorService.execute(() -> this.readComplete(bufList));
    }

    private void readComplete(List<Pair<PositionImpl, ByteBuf>> list) {
        log.info("xxxx {} Read entries complete. Entries size: {}", name, list.size());
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        for (Pair<PositionImpl, ByteBuf> entry : list) {
            PENDING_SIZE_UPDATER.incrementAndGet(this);
            readProcess(entry.getRight(), entry.getLeft()).whenCompleteAsync((ignored, exception) -> {
                if (exception != null) {
                    log.error("{} Error producing messages", name, exception);
                    markDeletePositions();
                    this.cursor.rewind();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Route message successfully.", name);
                    }
                    log.info("{} Route message successfully {}.", name, entry.getLeft());
                    this.markDeletePositionDeque.add(entry.getLeft());
                }
                if (PENDING_SIZE_UPDATER.decrementAndGet(this) == 0
                        && HAVE_PENDING_READ_UPDATER.get(this) == FALSE) {
                    log.info("xxxx read more entries after read complete");
                    this.readMoreEntries();
                }
            });
            entry.getRight().release();
        }
    }

    public abstract CompletableFuture<Void> readProcess(ByteBuf entry, Position position);

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object o) {
        if (exception instanceof ManagedLedgerException.CursorAlreadyClosedException) {
            log.error("[{}] Error reading entries because cursor is already closed.", name, exception);
            cursor.setInactive();
            cursor = null;
            stopReplicate();
            return;
        }

        long waitTimeMs = readFailureBackoff.next();
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries from bookie failed, retrying in {} s", name, waitTimeMs / 1000, exception);
        }
        log.info("xxxx {} Read entries from bookie failed, retrying in {} s", name, waitTimeMs / 1000, exception);
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        scheduledExecutorService.schedule(this::readMoreEntries, waitTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void deleteComplete(Object positions) {
        if (log.isDebugEnabled()) {
            log.debug("{} Deleted message at {}", name, positions);
        }
    }

    @Override
    public void deleteFailed(ManagedLedgerException e, Object positions) {
        log.error("{} Failed to delete message at {}: {}", name, positions, e.getMessage(), e);
        markDeletePositions();
        this.cursor.rewind();
    }

    public void stopReplicate() {
        if (cursor == null) {
            STATE_UPDATER.set(this, State.Stopped);
            log.info("[{}] AMQP Exchange Replicator is stopped.", name);
            return;
        }

        if (STATE_UPDATER.get(this).equals(State.Stopping)) {
            log.warn("Replicator is stopping.");
            return;
        }

        if (cursor != null && (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopping)
                || STATE_UPDATER.compareAndSet(this, State.Started, State.Stopping))) {
            cursor.setInactive();
            if (!this.markDeleteExecutor.isShutdown() && !this.markDeleteExecutor.isTerminated()) {
                markDeletePositions();
                this.markDeleteExecutor.shutdown();
            }
            cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object o) {
                    log.info("[{}] AMQP Exchange Replicator is stopped.", name);
                    STATE_UPDATER.set(AmqpExchangeReplicator.this, State.Stopped);
                    cursor = null;
                }

                @Override
                public void closeFailed(ManagedLedgerException e, Object o) {
                    if (e instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                        cursor = null;
                        STATE_UPDATER.set(AmqpExchangeReplicator.this, State.Stopped);
                        return;
                    }
                    long waitTimeMs = backOff.next();
                    log.error("[{}] AMQP Exchange Replicator stop failed. retrying in {} s",
                            name, waitTimeMs / 1000, e);
                    AmqpExchangeReplicator.this.scheduledExecutorService.schedule(
                            AmqpExchangeReplicator.this::stopReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
                }
            }, null);
        }

        log.info("[{}] AMQP Exchange Replicator is already stopped. State: {}", name, STATE_UPDATER.get(this));
    }

    private synchronized void markDeletePositions() {
        if (markDeletePositionDeque.isEmpty()) {
            return;
        }
        int size = markDeletePositionDeque.size();
        List<Position> positions = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Position position = markDeletePositionDeque.pollFirst();
            if (position == null) {
                break;
            }
            positions.add(position);
        }
        this.cursor.asyncDelete(positions, this, positions);
    }

}
