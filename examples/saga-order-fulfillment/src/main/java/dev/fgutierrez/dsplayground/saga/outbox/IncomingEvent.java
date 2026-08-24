package dev.fgutierrez.dsplayground.saga.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;

/**
 * What every consumer in this choreography needs out of an EventEnvelope, plus the raw payload for
 * whatever event-specific fields that particular listener cares about (totalAmount, reason, ...) —
 * one generic parser instead of a typed one per event type, since every payload here carries
 * orderId.
 */
public record IncomingEvent(UUID eventId, String type, UUID orderId, JsonNode payload) {

  public static IncomingEvent parse(String json, ObjectMapper objectMapper) {
    try {
      JsonNode root = objectMapper.readTree(json);
      UUID eventId = UUID.fromString(root.get("eventId").asText());
      String type = root.get("type").asText();
      JsonNode payload = root.get("payload");
      UUID orderId = UUID.fromString(payload.get("orderId").asText());
      return new IncomingEvent(eventId, type, orderId, payload);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Malformed event payload", e);
    }
  }
}
