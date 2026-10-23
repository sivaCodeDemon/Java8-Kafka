package com.example.kafkaecommerce.payment.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments", uniqueConstraints = @UniqueConstraint(name = "uk_payment_order", columnNames = "order_id"))
public class PaymentEntity {
    @Id
    private UUID id;
    @Column(name = "order_id", nullable = false, unique = true)
    private UUID orderId;
    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;
    @Column(nullable = false, length = 3)
    private String currency;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;
    private String failureReason;
    @Column(nullable = false)
    private Instant createdAt;
    protected PaymentEntity() {
    }

    public PaymentEntity(UUID id, UUID orderId, BigDecimal amount, String currency, PaymentStatus status,
                         String failureReason) {
        this.id = id;
        this.orderId = orderId;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.failureReason = failureReason;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public UUID getOrderId() { return orderId; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public PaymentStatus getStatus() { return status; }
    public String getFailureReason() { return failureReason; }
    public Instant getCreatedAt() { return createdAt; }

    public void refund() {
        if (status != PaymentStatus.SUCCEEDED) throw new IllegalStateException("Only successful payments can be refunded");
        status = PaymentStatus.REFUNDED;
        Instant.now();
    }
}
