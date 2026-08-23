package dev.fgutierrez.dsplayground.syncprocessing;

import static org.assertj.core.api.Assertions.assertThat;

import dev.fgutierrez.dsplayground.syncprocessing.support.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;

class SynchronousProcessingApplicationTests extends PostgresIntegrationTest {

  @Autowired private HealthEndpoint healthEndpoint;

  @Test
  void contextLoadsAndDatabaseIsHealthy() {
    assertThat(healthEndpoint.health().getStatus()).isEqualTo(Status.UP);
  }
}
