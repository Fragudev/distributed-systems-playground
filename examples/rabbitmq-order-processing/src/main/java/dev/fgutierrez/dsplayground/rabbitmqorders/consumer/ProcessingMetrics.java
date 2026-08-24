package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * The same three metric names, with the same tags, exist in kafka-order-processing — that's
 * deliberate: it's what lets one Grafana panel plot both brokers side by side (differentiated by
 * the `broker` tag every metric gets automatically from `management.metrics.tags.broker` in
 * application.yml). See docs/diagrams/kafka-vs-rabbitmq-dashboard.md.
 */
@Component
class ProcessingMetrics {

  private static final String EVENTS_TOTAL = "order_processing_events_total";

  private final MeterRegistry registry;

  ProcessingMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  void recordProcessed(String consumerGroup) {
    registry
        .counter(EVENTS_TOTAL, "consumer_group", consumerGroup, "outcome", "processed")
        .increment();
  }

  void recordDuplicate(String consumerGroup) {
    registry
        .counter(EVENTS_TOTAL, "consumer_group", consumerGroup, "outcome", "duplicate")
        .increment();
  }

  void recordFailed(String consumerGroup) {
    registry
        .counter(EVENTS_TOTAL, "consumer_group", consumerGroup, "outcome", "failed")
        .increment();
  }
}
