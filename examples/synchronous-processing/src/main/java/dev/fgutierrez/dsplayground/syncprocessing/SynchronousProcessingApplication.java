package dev.fgutierrez.dsplayground.syncprocessing;

import java.time.Clock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SynchronousProcessingApplication {

  public static void main(String[] args) {
    SpringApplication.run(SynchronousProcessingApplication.class, args);
  }

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }
}
