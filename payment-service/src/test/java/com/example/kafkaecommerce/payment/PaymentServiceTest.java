package com.example.kafkaecommerce.payment;

import com.example.kafkaecommerce.events.*;
import com.example.kafkaecommerce.payment.entity.PaymentEntity;
import com.example.kafkaecommerce.payment.entity.PaymentStatus;
import com.example.kafkaecommerce.payment.repository.PaymentRepository;
import com.example.kafkaecommerce.payment.service.PaymentService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {
    @Mock PaymentRepository payments;
    @Mock OutboxWriter outbox;
    @Mock Inbox inbox;
    @Mock JdbcTemplate jdbc;

    private PaymentService service;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    @BeforeEach
    void setUp() {
        service = new PaymentService(payments, outbox, inbox, mapper, jdbc);
    }

    @Test
    void successfulPaymentUsesStablePaymentIdAndPublishesOutcome() throws Exception {
        UUID orderId = UUID.randomUUID();
        Events.InventoryReserved reserved = reserved(orderId, false);
        when(inbox.firstDelivery("payment-inventory-reserved", reserved.eventId())).thenReturn(true);
        stubOrderLockAndCancellation();
        when(payments.findByOrderId(orderId)).thenReturn(Optional.empty());

        service.onInventoryReserved(mapper.writeValueAsString(reserved));

        ArgumentCaptor<PaymentEntity> payment = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(payments).save(payment.capture());
        assertEquals(PaymentStatus.SUCCEEDED, payment.getValue().getStatus());
        assertEquals(UUID.nameUUIDFromBytes(("payment:" + orderId).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                payment.getValue().getId());
        ArgumentCaptor<DomainEvent> event = ArgumentCaptor.forClass(DomainEvent.class);
        verify(outbox).append(eq(Topics.PAYMENT_SUCCEEDED), eq(orderId.toString()), event.capture());
        assertInstanceOf(Events.PaymentSucceeded.class, event.getValue());
    }

    @Test
    void simulatedDeclinePersistsFailureAndPublishesCompensationTrigger() throws Exception {
        UUID orderId = UUID.randomUUID();
        Events.InventoryReserved reserved = reserved(orderId, true);
        when(inbox.firstDelivery("payment-inventory-reserved", reserved.eventId())).thenReturn(true);
        stubOrderLockAndCancellation();
        when(payments.findByOrderId(orderId)).thenReturn(Optional.empty());

        service.onInventoryReserved(mapper.writeValueAsString(reserved));

        ArgumentCaptor<PaymentEntity> payment = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(payments).save(payment.capture());
        assertEquals(PaymentStatus.FAILED, payment.getValue().getStatus());
        ArgumentCaptor<DomainEvent> event = ArgumentCaptor.forClass(DomainEvent.class);
        verify(outbox).append(eq(Topics.PAYMENT_FAILED), eq(orderId.toString()), event.capture());
        assertInstanceOf(Events.PaymentFailed.class, event.getValue());
    }

    @Test
    void malformedEventFailsBeforeItIsMarkedProcessed() {
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class,
                () -> service.onInventoryReserved("{invalid"));
        verifyNoInteractions(inbox, payments, outbox);
    }

    @Test
    void successfulPaymentCanBeRefundedExactlyOnce() {
        UUID paymentId = UUID.randomUUID();
        PaymentEntity payment = new PaymentEntity(paymentId, UUID.randomUUID(),
                new BigDecimal("19.99"), "USD", PaymentStatus.SUCCEEDED, null);
        when(payments.findByIdForUpdate(paymentId)).thenReturn(Optional.of(payment));

        PaymentEntity result = service.refund(paymentId, "correlation-2");
        assertEquals(PaymentStatus.REFUNDED, result.getStatus());
        verify(outbox).append(eq(Topics.PAYMENT_REFUNDED), eq(payment.getOrderId().toString()),
                isA(Events.PaymentRefunded.class));
    }

    @Test
    void failedPaymentCannotBeRefunded() {
        UUID paymentId = UUID.randomUUID();
        PaymentEntity payment = new PaymentEntity(paymentId, UUID.randomUUID(),
                new BigDecimal("19.99"), "USD", PaymentStatus.FAILED, "declined");
        when(payments.findByIdForUpdate(paymentId)).thenReturn(Optional.of(payment));

        assertThrows(org.springframework.web.server.ResponseStatusException.class,
                () -> service.refund(paymentId, "correlation-3"));
        verifyNoInteractions(outbox);
    }

    private void stubOrderLockAndCancellation() {
        when(jdbc.queryForObject(eq("SELECT pg_advisory_xact_lock(hashtext(?))"),
                eq(Object.class), any(Object[].class))).thenReturn(null);
        when(jdbc.queryForObject(contains("cancelled_orders"), eq(Integer.class),
                any(Object[].class))).thenReturn(0);
    }

    private Events.InventoryReserved reserved(UUID orderId, boolean shouldFail) {
        return new Events.InventoryReserved(UUID.randomUUID(), "correlation-1", Instant.now(),
                orderId, "sku-1", 1, new BigDecimal("19.99"), "USD", shouldFail);
    }
}
