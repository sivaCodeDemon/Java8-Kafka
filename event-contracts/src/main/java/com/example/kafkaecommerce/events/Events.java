package com.example.kafkaecommerce.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class Events {
    private Events() {
    }

    public record UserCreated(UUID eventId, String correlationId, Instant occurredAt, UUID userId,
                              String email, String fullName) implements DomainEvent {
    }

    public record UserUpdated(UUID eventId, String correlationId, Instant occurredAt, UUID userId,
                              String email, String fullName) implements DomainEvent {
    }

    public record OrderCreated(UUID eventId, String correlationId, Instant occurredAt, UUID orderId,
                               UUID userId, String productId, int quantity, BigDecimal amount,
                               String currency, boolean simulatePaymentFailure) implements DomainEvent {
    }

    public record OrderCancelled(UUID eventId, String correlationId, Instant occurredAt,
                                 UUID orderId) implements DomainEvent {
    }

    public record OrderStatusChanged(UUID eventId, String correlationId, Instant occurredAt,
                                     UUID orderId, String status, String reason) implements DomainEvent {
    }

    public record InventoryReserved(UUID eventId, String correlationId, Instant occurredAt,
                                    UUID orderId, String productId, int quantity, BigDecimal amount,
                                    String currency, boolean simulatePaymentFailure) implements DomainEvent {
    }

    public record InventoryRejected(UUID eventId, String correlationId, Instant occurredAt,
                                    UUID orderId, String productId, String reason) implements DomainEvent {
    }

    public record InventoryReleased(UUID eventId, String correlationId, Instant occurredAt,
                                    UUID orderId, String productId, int quantity) implements DomainEvent {
    }

    public record PaymentSucceeded(UUID eventId, String correlationId, Instant occurredAt,
                                   UUID orderId, UUID paymentId, BigDecimal amount,
                                   String currency) implements DomainEvent {
    }

    public record PaymentFailed(UUID eventId, String correlationId, Instant occurredAt,
                                UUID orderId, UUID paymentId, String reason) implements DomainEvent {
    }

    public record PaymentRefunded(UUID eventId, String correlationId, Instant occurredAt,
                                  UUID orderId, UUID paymentId, BigDecimal amount) implements DomainEvent {
    }
}
