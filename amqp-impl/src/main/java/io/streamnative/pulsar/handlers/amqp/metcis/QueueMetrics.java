package io.streamnative.pulsar.handlers.amqp.metcis;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import lombok.Getter;


public interface QueueMetrics {

    String getVhost();

    String getQueueName();

    void writeInc();

    void writeSuccessInc();

    void writeFailed();

    Histogram.Timer startWrite();

    void finishWrite(Histogram.Timer timer);

    void readInc();

    void readFailed();

    Histogram.Timer startRead();

    void finishRead(Histogram.Timer timer);

    void ackInc();

    void collect();

    static class QueueMetricsImpl implements QueueMetrics {

        @Getter
        private final String vhost;
        @Getter
        private final String queueName;
//        private final static String[] LABELS = {"vhost", "queue_name"};
        private final static String[] LABELS = {"vhost"};
        private final String[] labelValues;

        private static final Counter writeCounter = Counter.build()
                .name("queue_publish_counter")
                .labelNames(LABELS)
                .help("Queue publish counter.").register();

        private static final Counter writeSuccessCounter = Counter.build()
                .name("queue_publish_success_counter")
                .labelNames(LABELS)
                .help("Queue publish success counter.").register();

        static final Counter writeFailedCounter = Counter.build()
                .name("queue_publish_failed_counter")
                .labelNames(LABELS)
                .help("Queue write failed counter.").register();

        static final Histogram writeLatency = Histogram.build()
                .name("queue_write_latency_seconds")
                .labelNames(LABELS)
                .help("Queue write latency in seconds.").register();

        static final Counter readCounter = Counter.build()
                .name("queue_read_counter")
                .labelNames(LABELS)
                .help("Queue read counter.").register();

        static final Counter readFailedCounter = Counter.build()
                .name("queue_read_failed_counter")
                .labelNames(LABELS)
                .help("Queue read failed counter.").register();

        static final Histogram readLatency = Histogram.build()
                .name("queue_read_latency_seconds")
                .labelNames(LABELS)
                .help("Queue read latency in seconds.").register();

        static final Counter ackCounter = Counter.build()
                .name("queue_ack_counter")
                .labelNames(LABELS)
                .help("Queue ack counter.").register();

        public QueueMetricsImpl(String vhost, String queueName) {
            this.vhost = vhost;
            this.queueName = queueName;
//            this.labelValues = new String[]{vhost, queueName};
            this.labelValues = new String[]{vhost};
        }

        public void writeInc() {
            writeCounter.labels(labelValues).inc();
        }

        public void writeSuccessInc() {
            writeSuccessCounter.labels(labelValues).inc();
        }

        public void writeFailed() {
            writeFailedCounter.labels(labelValues).inc();
        }

        public Histogram.Timer startWrite() {
            return writeLatency.labels(labelValues).startTimer();
        }

        @Override
        public void finishWrite(Histogram.Timer timer) {
            timer.observeDuration();
        }

        public void readInc() {
            readCounter.labels(labelValues).inc();
        }

        public void readFailed() {
            readFailedCounter.labels(labelValues).inc();
        }

        public Histogram.Timer startRead() {
            return readLatency.labels(labelValues).startTimer();
        }

        @Override
        public void finishRead(Histogram.Timer timer) {
            timer.observeDuration();
        }

        public void ackInc() {
            ackCounter.labels(labelValues).inc();
        }

        public void collect() {
            writeCounter.collect();
            writeSuccessCounter.collect();
            writeFailedCounter.collect();
            writeLatency.collect();
            readCounter.collect();
            readFailedCounter.collect();
            readLatency.collect();
            ackCounter.collect();
        }

    }

    static class QueueMetricsDisable implements QueueMetrics {
        @Override
        public String getVhost() {
            return null;
        }

        @Override
        public String getQueueName() {
            return null;
        }

        @Override
        public void writeInc() {

        }

        @Override
        public void writeSuccessInc() {

        }

        @Override
        public void writeFailed() {

        }

        @Override
        public Histogram.Timer startWrite() {
            return null;
        }

        @Override
        public void finishWrite(Histogram.Timer timer) {

        }

        @Override
        public void readInc() {

        }

        @Override
        public void readFailed() {

        }

        @Override
        public Histogram.Timer startRead() {
            return null;
        }

        @Override
        public void finishRead(Histogram.Timer timer) {

        }

        @Override
        public void ackInc() {

        }

        @Override
        public void collect() {

        }
    }

    static QueueMetrics create(boolean enableMetrics, String vhost, String queueName) {
        if (enableMetrics) {
            return new QueueMetricsImpl(vhost, queueName);
        }
        return new QueueMetricsDisable();
    }

}
