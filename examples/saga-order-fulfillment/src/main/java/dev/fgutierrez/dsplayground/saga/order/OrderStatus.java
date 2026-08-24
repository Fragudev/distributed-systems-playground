package dev.fgutierrez.dsplayground.saga.order;

/**
 * Unlike every other example's order domain, this one actually advances past CREATED — the whole
 * point of the saga is deciding which of the other two states an order ends up in.
 */
public enum OrderStatus {
  CREATED,
  CONFIRMED,
  CANCELLED
}
