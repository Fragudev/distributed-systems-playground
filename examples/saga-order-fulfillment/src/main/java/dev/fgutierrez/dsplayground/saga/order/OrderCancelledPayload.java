package dev.fgutierrez.dsplayground.saga.order;

import java.util.UUID;

/**
 * The `payload` of an `order.cancelled.v1` EventEnvelope. `reason` is observability, not a routing
 * decision: payment-service and inventory-service each decide whether *they* have something to
 * compensate by checking their own local state for this order, not by reading which leg caused the
 * cancellation — see the example README §3.
 */
public record OrderCancelledPayload(UUID orderId, String reason) {}
