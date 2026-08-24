package dev.fgutierrez.dsplayground.rabbitmqorders.order;

/**
 * Same minimal domain as the synchronous-processing example — see its ADR 0002 for why it's
 * duplicated instead of shared. This example never advances past CREATED either.
 */
public enum OrderStatus {
  CREATED
}
