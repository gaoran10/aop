package io.streamnative.pulsar.handlers.amqp.metcis;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import lombok.Getter;

public interface ExchangeMetrics extends MetadataMetrics {

    String getVhost();

    String getExchangeName();

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

    void routeInc();

    void routeFailedInc();

    Histogram.Timer startRoute();

    void finishRoute(Histogram.Timer timer);

//    void collect();

    class ExchangeMetricsImpl implements ExchangeMetrics {
        @Getter
        private final String vhost;
        @Getter
        private final String exchangeName;
        private final static String[] LABELS = {"vhost", "exchange_name"};
        private final String[] labelValues;

        static final Counter writeCounter = Counter.build()
                .name("exchange_write_counter")
                .labelNames(LABELS)
                .help("Exchange write counter.").register();

        static final Counter writeSuccessCounter = Counter.build()
                .name("exchange_write_success_counter")
                .labelNames(LABELS)
                .help("Exchange write success counter.").register();

        static final Counter writeFailedCounter = Counter.build()
                .name("exchange_write_failed_counter")
                .labelNames(LABELS)
                .help("Exchange write failed counter.").register();

        static final Histogram writeLatency = Histogram.build()
                .name("exchange_write_latency_seconds")
                .labelNames(LABELS)
                .help("Exchange write latency in seconds.").register();

        static final Counter readCounter = Counter.build()
                .name("exchange_read_counter")
                .labelNames(LABELS)
                .help("Exchange read counter.").register();

        static final Counter readFailedCounter = Counter.build()
                .name("exchange_read_failed_counter")
                .labelNames(LABELS)
                .help("Exchange read failed counter.").register();

        static final Histogram readLatency = Histogram.build()
                .name("exchange_read_latency_seconds")
                .labelNames(LABELS)
                .help("Exchange read latency in seconds.").register();

        static final Counter ackCounter = Counter.build()
                .name("exchange_ack_counter")
                .labelNames(LABELS)
                .help("Exchange ack counter.").register();

        static final Counter routeCounter = Counter.build()
                .name("exchange_route_counter")
                .labelNames(LABELS)
                .help("Exchange route counter").register();

        static final Counter routeSuccessCounter = Counter.build()
                .name("exchange_route_success_counter")
                .labelNames(LABELS)
                .help("Exchange route success counter").register();

        static final Counter routeFailedCounter = Counter.build()
                .name("exchange_route_failed_counter")
                .labelNames(LABELS)
                .help("Exchange route counter").register();

        static final Histogram routeLatency = Histogram.build()
                .name("exchange_route_latency")
                .labelNames(LABELS)
                .help("Exchange route latency").register();

        static final Counter metadataUpdateFailedCounter = Counter.build()
                .name("exchange_metadata_update_counter")
                .labelNames(LABELS)
                .help("Exchange metadata update counter").register();

        public ExchangeMetricsImpl(String vhost, String exchangeName) {
            this.vhost = vhost;
            this.exchangeName = exchangeName;
            this.labelValues = new String[]{vhost, exchangeName};
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

        public void routeInc() {
            routeCounter.labels(labelValues).inc();
        }

        public void routeFailedInc() {
            routeFailedCounter.labels(labelValues).inc();
        }

        public Histogram.Timer startRoute() {
            return routeLatency.labels(labelValues).startTimer();
        }

        @Override
        public void finishRoute(Histogram.Timer timer) {
            timer.observeDuration();
        }

        @Override
        public void metadataUpdateFailedInc() {
            metadataUpdateFailedCounter.labels(labelValues).inc();
        }

        //        public void collect() {
//            writeCounter.collect();
////            writeSuccessCounter.collect();
//            writeFailedCounter.collect();
////            writeLatency.collect();
//            readCounter.collect();
//            readFailedCounter.collect();
////            readLatency.collect();
//            ackCounter.collect();
//            routeCounter.collect();
//            routeFailedCounter.collect();
////            routeLatency.collect();
//        }
    }

    static class ExchangeMetricsDisable implements ExchangeMetrics {

        @Override
        public String getVhost() {
            return null;
        }

        @Override
        public String getExchangeName() {
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
        public void routeInc() {

        }

        @Override
        public void routeFailedInc() {

        }

        @Override
        public Histogram.Timer startRoute() {
            return null;
        }

        @Override
        public void finishRoute(Histogram.Timer timer) {

        }

        @Override
        public void metadataUpdateFailedInc() {

        }

        //        @Override
//        public void collect() {
//
//        }

    }

    static ExchangeMetrics create(boolean enableMetrics, String vhost, String exchangeName) {
        if (enableMetrics) {
            return new ExchangeMetricsImpl(vhost, exchangeName);
        }
        return new ExchangeMetricsDisable();
    }

}
