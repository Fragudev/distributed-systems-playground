package dev.fgutierrez.dsplayground.saga.inventory;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {

  Optional<InventoryReservation> findByOrderId(UUID orderId);
}
