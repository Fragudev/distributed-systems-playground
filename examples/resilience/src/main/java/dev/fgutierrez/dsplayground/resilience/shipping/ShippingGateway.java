package dev.fgutierrez.dsplayground.resilience.shipping;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface ShippingGateway {

  CompletableFuture<ShippingConfirmation> requestShipment(UUID orderId);
}
