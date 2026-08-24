package dev.fgutierrez.dsplayground.saga.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Every publishing component in this example (order-service, payment-service, inventory-service,
 * the saga coordinator) needs this exact serialization — shared once here instead of four times,
 * unlike the per-example EventEnvelope duplication ADR 0002 calls for across examples.
 */
@Component
public class EventEnvelopeWriter {

  private final ObjectMapper objectMapper;

  public EventEnvelopeWriter(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public String write(EventEnvelope envelope) {
    try {
      return objectMapper.writeValueAsString(envelope);
    } catch (JsonProcessingException e) {
      // Only possible here if EventEnvelope stops being trivially serializable — a programming
      // error, not a runtime condition callers should have to handle.
      throw new IllegalStateException("Failed to serialize event envelope", e);
    }
  }
}
