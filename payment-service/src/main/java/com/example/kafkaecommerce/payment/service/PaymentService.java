package com.example.kafkaecommerce.payment.service;

import com.example.kafkaecommerce.events.*;
import com.example.kafkaecommerce.payment.entity.PaymentEntity;
import com.example.kafkaecommerce.payment.entity.PaymentStatus;
import com.example.kafkaecommerce.payment.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentService {
    private final PaymentRepository payments;
    private final OutboxWriter outbox;
    private final Inbox inbox;
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;

    public PaymentService(PaymentRepository payments, OutboxWriter outbox, Inbox inbox,
                          ObjectMapper mapper, JdbcTemplate jdbc) {
        this.payments = payments;
        this.outbox = outbox;
        this.inbox = inbox;
        this.mapper = mapper;
        this.jdbc = jdbc;
    }

    @Transactional
    public void onInventoryReserved(String payload) throws Exception {
        Events.InventoryReserved event = mapper.readValue(payload, Events.InventoryReserved.class);
        if (!inbox.firstDelivery("payment-inventory-reserved", event.eventId())) return;
        lockOrder(event.orderId());
        Integer cancelled = jdbc.queryForObject("SELECT count(*) FROM cancelled_orders WHERE order_id = ?",
                Integer.class, event.orderId());
        if (cancelled != null && cancelled > 0) return;
        if (payments.findByOrderId(event.orderId()).isPresent()) return;
        UUID paymentId = UUID.nameUUIDFromBytes(("payment:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        PaymentEntity payment;
        if (event.simulatePaymentFailure()) {
            payment = new PaymentEntity(paymentId, event.orderId(), event.amount(), event.currency(),
                    PaymentStatus.FAILED, "Simulated payment decline");
            payments.save(payment);
            outbox.append(Topics.PAYMENT_FAILED, event.orderId().toString(),
                    new Events.PaymentFailed(UUID.randomUUID(), event.correlationId(), Instant.now(),
                            event.orderId(), paymentId, payment.getFailureReason()));
        } else {
            payment = new PaymentEntity(paymentId, event.orderId(), event.amount(), event.currency(),
                    PaymentStatus.SUCCEEDED, null);
            payments.save(payment);
            outbox.append(Topics.PAYMENT_SUCCEEDED, event.orderId().toString(),
                    new Events.PaymentSucceeded(UUID.randomUUID(), event.correlationId(), Instant.now(),
                            event.orderId(), paymentId, event.amount(), event.currency()));
        }
    }

    @Transactional
    public void onOrderCancelled(String payload) throws Exception {
        Events.OrderCancelled event = mapper.readValue(payload, Events.OrderCancelled.class);
        if (!inbox.firstDelivery("payment-order-cancelled", event.eventId())) return;
        lockOrder(event.orderId());
        jdbc.update("INSERT INTO cancelled_orders(order_id) VALUES (?) ON CONFLICT DO NOTHING", event.orderId());
        payments.findByOrderIdForUpdate(event.orderId())
                .ifPresent(payment -> refund(payment, event.correlationId()));
    }

    @Transactional(readOnly = true)
    public PaymentEntity get(UUID paymentId) {
        return payments.findById(paymentId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found"));
    }

    @Transactional(readOnly = true)
    public PaymentEntity getByOrderId(UUID orderId) {
        return payments.findByOrderId(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found"));
    }

    @Transactional
    public PaymentEntity refund(UUID paymentId, String correlationId) {
        PaymentEntity payment = payments.findByIdForUpdate(paymentId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found"));
        if (payment.getStatus() == PaymentStatus.REFUNDED) return payment;
        if (payment.getStatus() != PaymentStatus.SUCCEEDED) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Only a successful payment can be refunded");
        }
        refund(payment, correlationId);
        return payment;
    }

    private void refund(PaymentEntity payment, String correlationId) {
        if (payment.getStatus() != PaymentStatus.SUCCEEDED) return;
        payment.refund();
        outbox.append(Topics.PAYMENT_REFUNDED, payment.getOrderId().toString(),
                new Events.PaymentRefunded(UUID.randomUUID(), correlationId, Instant.now(),
                        payment.getOrderId(), payment.getId(), payment.getAmount()));
    }

    private void lockOrder(UUID orderId) {
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))", Object.class, orderId.toString());
    }
}
