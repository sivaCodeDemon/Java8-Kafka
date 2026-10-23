package com.example.kafkaecommerce.order.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class OrderEntity {
    @Id
    private UUID id;
    @Column(nullable = false)
    private UUID userId;
    @Column(nullable = false)
    private String productId;
    @Column(nullable = false)
    private int quantity;
    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;
    @Column(nullable = false, length = 3)
    private String currency;
    @Column(nullable = false)
    private boolean simulatePaymentFailure;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;
    private String failureReason;
    @Column(nullable = false)
    private Instant createdAt;
    @Column(nullable = false)
    private Instant updatedAt;

    protected OrderEntity() {
    }

    public OrderEntity(UUID id, UUID userId, String productId, int quantity, BigDecimal amount,
                       String currency, boolean simulatePaymentFailure) {
        this.id = id;
        this.userId = userId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.currency = currency;
        this.simulatePaymentFailure = simulatePaymentFailure;
        this.status = OrderStatus.PENDING;
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public UUID getId() { return id; }
    public UUID getUserId() { return userId; }
    public String getProductId() { return productId; }
    public int getQuantity() { return quantity; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public boolean isSimulatePaymentFailure() { return simulatePaymentFailure; }
    public OrderStatus getStatus() { return status; }
    public String getFailureReason() { return failureReason; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public boolean transition(OrderStatus next, String reason) {
        if (status == OrderStatus.CANCELLED || status == OrderStatus.FAILED) return false;
        if (status == OrderStatus.CONFIRMED && next != OrderStatus.CANCELLED) return false;
        boolean allowed = switch (next) {
            case INVENTORY_RESERVED -> status == OrderStatus.PENDING;
            case PAYMENT_PENDING -> status == OrderStatus.PENDING || status == OrderStatus.INVENTORY_RESERVED;
            case CONFIRMED -> status == OrderStatus.PENDING
                    || status == OrderStatus.PAYMENT_PENDING
                    || status == OrderStatus.INVENTORY_RESERVED;
            case FAILED -> status != OrderStatus.CONFIRMED;
            case CANCELLED -> true;
            case PENDING -> false;
        };
        if (!allowed) return false;
        status = next;
        failureReason = reason;
        updatedAt = Instant.now();
        return true;
    }
}
