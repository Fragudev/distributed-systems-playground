package dev.fgutierrez.dsplayground.rabbitmqorders.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;

/**
 * The bits every consumer needs out of an `order.created.v1` EventEnvelope, without redefining the
 * whole envelope shape a second time just to read two fields off it.
 */
record IncomingOrderCreatedEvent(UUID eventId, UUID orderId) {

  static IncomingOrderCreatedEvent parse(String json, ObjectMapper objectMapper) {
    try {
      JsonNode root = objectMapper.readTree(json);
      UUID eventId = UUID.fromString(root.get("eventId").asText());
      UUID orderId = UUID.fromString(root.get("payload").get("orderId").asText());
      return new IncomingOrderCreatedEvent(eventId, orderId);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Malformed order.created.v1 payload", e);
    }
  }
}
