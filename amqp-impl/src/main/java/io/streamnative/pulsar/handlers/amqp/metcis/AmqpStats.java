package io.streamnative.pulsar.handlers.amqp.metcis;


import io.prometheus.client.Gauge;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface AmqpStats {

    void connectionInc(String vhost);

    void connectionDec(String vhost);

    ExchangeMetrics addExchangeMetrics(String vhost, String exchangeName);

    void deleteExchangeMetrics(String vhost, String exchangeName);

    QueueMetrics addQueueMetrics(String vhost, String queueName);

    void deleteQueueMetrics(String vhost, String queueName);

//    void collect();

    static class AmqpStatsImpl implements AmqpStats {
        private final boolean enableMetrics;
        private final Map<String, ConnectionMetrics> connectionMetricsMap = new ConcurrentHashMap<>();
        private final Map<String, Map<String, ExchangeMetrics>> exchangeMetricsMap = new ConcurrentHashMap<>();
        private final Map<String, Map<String, QueueMetrics>> queueMetricsMap = new ConcurrentHashMap<>();

        static Gauge vhostGauge = Gauge.build()
                .name("vhost_count")
                .help("Vhost count").register();

        static Gauge exchangeGauge = Gauge.build()
                .name("exchange_count")
                .labelNames("vhost")
                .help("Exchange count").register();

        static Gauge queueGauge = Gauge.build()
                .name("queue_count")
                .labelNames("vhost")
                .help("Queue count").register();

        public AmqpStatsImpl(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
        }

        public void connectionInc(String vhost) {
            connectionMetricsMap.computeIfAbsent(vhost, k -> ConnectionMetrics.create(enableMetrics, vhost)).inc();
            updateVhostGauge();
        }

        public void connectionDec(String vhost) {
            connectionMetricsMap.computeIfPresent(vhost, (k, metrics) -> {
                metrics.dec();
                return metrics;
            });
            updateVhostGauge();
        }

        public ExchangeMetrics addExchangeMetrics(String vhost, String exchangeName) {
            ExchangeMetrics exchangeMetrics = ExchangeMetrics.create(this.enableMetrics, vhost, exchangeName);
            exchangeMetricsMap.computeIfAbsent(exchangeMetrics.getVhost(), k -> new ConcurrentHashMap<>())
                    .putIfAbsent(exchangeMetrics.getExchangeName(), exchangeMetrics);
            updateExchangeGauge(vhost);
            return exchangeMetrics;
        }

        public void deleteExchangeMetrics(String vhost, String exchangeName) {
            exchangeMetricsMap.computeIfPresent(vhost, (key, map) -> {
                map.remove(exchangeName);
                return map;
            });
            updateExchangeGauge(vhost);
        }

        public QueueMetrics addQueueMetrics(String vhost, String queueName) {
            QueueMetrics queueMetrics = QueueMetrics.create(this.enableMetrics, vhost, queueName);
            queueMetricsMap.computeIfAbsent(queueMetrics.getVhost(), k -> new ConcurrentHashMap<>())
                    .putIfAbsent(queueMetrics.getQueueName(), queueMetrics);
            updateQueueGauge(vhost);
            return queueMetrics;
        }

        public void deleteQueueMetrics(String vhost, String queueName) {
            queueMetricsMap.computeIfPresent(vhost, (key, map) -> {
                map.remove(queueName);
                return map;
            });
            updateQueueGauge(vhost);
        }

        private void updateVhostGauge() {
            vhostGauge.set(connectionMetricsMap.size());
        }

        private void updateExchangeGauge(String vhost) {
            exchangeGauge.labels(vhost)
                    .set(exchangeMetricsMap.getOrDefault(vhost, new HashMap<>(0)).size());
        }

        private void updateQueueGauge(String vhost) {
            queueGauge.labels(vhost)
                    .set(queueMetricsMap.getOrDefault(vhost, new HashMap<>(0)).size());
        }

//        public void collect() {
//            for (ConnectionMetrics metrics : connectionMetricsMap.values()) {
//                metrics.collect();
//            }
//            for (Map.Entry<String, Map<String, ExchangeMetrics>> metricsEntry : exchangeMetricsMap.entrySet()) {
//                if (metricsEntry == null) {
//                    continue;
//                }
//                exchangeGauge.labels(metricsEntry.getKey()).set(metricsEntry.getValue().size());
//                for (ExchangeMetrics metrics : metricsEntry.getValue().values()) {
//                    metrics.collect();
//                }
//            }
//            exchangeGauge.collect();
//            for (Map.Entry<String, Map<String, QueueMetrics>> metricsEntry : queueMetricsMap.entrySet()) {
//                if (metricsEntry == null) {
//                    continue;
//                }
//                queueGauge.labels(metricsEntry.getKey()).set(metricsEntry.getValue().size());
//                for (QueueMetrics metrics : metricsEntry.getValue().values()) {
//                    metrics.collect();
//                }
//            }
//            queueGauge.collect();
//        }
    }

    public static class AmqpStatsDisable implements AmqpStats {
        @Override
        public void connectionInc(String vhost) {

        }

        @Override
        public void connectionDec(String vhost) {

        }

        @Override
        public ExchangeMetrics addExchangeMetrics(String vhost, String exchangeName) {
            return ExchangeMetrics.create(false, vhost, exchangeName);
        }

        @Override
        public void deleteExchangeMetrics(String vhost, String exchangeName) {

        }

        @Override
        public QueueMetrics addQueueMetrics(String vhost, String queueName) {
            return QueueMetrics.create(false, vhost, queueName);
        }

        @Override
        public void deleteQueueMetrics(String vhost, String queueName) {

        }

//        @Override
//        public void collect() {
//
//        }
    }

    static AmqpStats create(boolean enableMetrics) {
        if (enableMetrics) {
            return new AmqpStatsImpl(enableMetrics);
        }
        return new AmqpStatsDisable();
    }

}
