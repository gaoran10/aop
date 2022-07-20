package io.streamnative.pulsar.handlers.amqp.metcis;

import io.prometheus.client.Gauge;
import lombok.Getter;

public interface ConnectionMetrics {

    String getVhost();

    void inc();

    void dec();

//    void collect();

    static ConnectionMetrics create(boolean enableMetrics, String vhost) {
        if (enableMetrics) {
            return new ConnectionMetricsImpl(vhost);
        }
        return new ConnectionMetricsDisable();
    }

    public static class ConnectionMetricsImpl implements ConnectionMetrics {

        @Getter
        private final String vhost;

        static final Gauge connection = Gauge.build()
                .name("amqp_connection_counter")
                .labelNames("vhost").help("Amqp connection count.").register();

        public ConnectionMetricsImpl(String vhost) {
            this.vhost = vhost;
        }

        public void inc() {
            connection.labels(vhost).inc();
        }

        public void dec() {
            connection.labels(vhost).dec();
        }

//        public void collect() {
//            connection.collect();
//        }

    }

    public static class ConnectionMetricsDisable implements ConnectionMetrics {
        @Override
        public String getVhost() {
            return null;
        }

        @Override
        public void inc() {

        }

        @Override
        public void dec() {

        }

//        @Override
//        public void collect() {
//
//        }
    }

}
