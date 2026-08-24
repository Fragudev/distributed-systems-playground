package dev.fgutierrez.dsplayground.saga.notification;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface NotificationLogRepository extends JpaRepository<NotificationLog, UUID> {

  Optional<NotificationLog> findByOrderId(UUID orderId);
}
