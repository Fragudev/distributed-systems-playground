package dev.fgutierrez.dsplayground.outbox.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;

public record OrderLineRequest(
    @NotBlank String productId, @Positive int quantity, @NotNull @Positive BigDecimal unitPrice) {}
