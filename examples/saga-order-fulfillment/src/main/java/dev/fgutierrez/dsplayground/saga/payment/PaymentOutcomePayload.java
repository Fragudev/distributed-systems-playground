package dev.fgutierrez.dsplayground.saga.payment;

import java.util.UUID;

/**
 * The `payload` of both payment.completed.v1 and payment.failed.v1 — which one it is comes from the
 * EventEnvelope's `type`, not a field in here.
 */
public record PaymentOutcomePayload(UUID orderId) {}
