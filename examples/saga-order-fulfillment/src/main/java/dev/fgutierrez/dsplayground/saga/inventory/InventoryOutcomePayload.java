package dev.fgutierrez.dsplayground.saga.inventory;

import java.util.UUID;

/**
 * The `payload` of both inventory.reserved.v1 and inventory.rejected.v1 — which one it is comes
 * from the EventEnvelope's `type`, not a field in here.
 */
public record InventoryOutcomePayload(UUID orderId) {}
