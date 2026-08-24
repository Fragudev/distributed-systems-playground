package dev.fgutierrez.dsplayground.kafkaorders.consumer;

/**
 * Seam between InventoryEventListener and whatever a real inventory system would be, so the
 * poison-message/DLT/replay scenario (PoisonMessageAndReplayTest) can simulate a downstream outage
 * deterministically instead of needing a message that's permanently unprocessable — the same "wrap
 * a real collaborator, swap it in tests" approach as EventPublisher in the outbox example.
 */
public interface InventoryAvailabilityChecker {

  boolean isAvailable();
}
