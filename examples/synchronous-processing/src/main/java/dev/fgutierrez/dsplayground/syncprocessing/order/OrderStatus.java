package dev.fgutierrez.dsplayground.syncprocessing.order;

/**
 * This example never leaves CREATED: there are no downstream consumers here to advance an order to
 * VALIDATED/CONFIRMED/CANCELLED (that lifecycle belongs to the messaging examples). Kept as an enum
 * rather than a hardcoded string so the API response shape matches what those examples will expose.
 */
public enum OrderStatus {
  CREATED
}
