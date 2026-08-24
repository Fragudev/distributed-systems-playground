package dev.fgutierrez.dsplayground.kafkaorders.consumer;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, ProcessedEventId> {

  List<ProcessedEvent> findByIdEventId(UUID eventId);
}
