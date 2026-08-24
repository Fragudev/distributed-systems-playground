package dev.fgutierrez.dsplayground.saga.saga;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaStateRepository extends JpaRepository<SagaState, UUID> {}
