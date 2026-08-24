package dev.fgutierrez.dsplayground.saga.outbox;

/**
 * Every event type this example's choreography uses, in one place — also each one's Kafka topic
 * name, since OutboxRelay publishes with event type as the topic.
 */
public final class EventTypes {

  public static final String ORDER_CREATED = "order.created.v1";
  public static final String PAYMENT_COMPLETED = "payment.completed.v1";
  public static final String PAYMENT_FAILED = "payment.failed.v1";
  public static final String INVENTORY_RESERVED = "inventory.reserved.v1";
  public static final String INVENTORY_REJECTED = "inventory.rejected.v1";
  public static final String ORDER_CANCELLED = "order.cancelled.v1";

  private EventTypes() {}
}
