package com.example.kafkaecommerce.order;

import com.example.kafkaecommerce.order.entity.OrderEntity;
import com.example.kafkaecommerce.order.entity.OrderStatus;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class OrderStateTest {
    @Test
    void followsSagaAndRejectsLateEventsAfterCancellation() {
        var order = new OrderEntity(UUID.randomUUID(), UUID.randomUUID(), "sku-1", 1,
                new BigDecimal("12.00"), "USD", false);
        assertTrue(order.transition(OrderStatus.INVENTORY_RESERVED, null));
        assertTrue(order.transition(OrderStatus.PAYMENT_PENDING, null));
        assertTrue(order.transition(OrderStatus.CANCELLED, null));
        assertFalse(order.transition(OrderStatus.CONFIRMED, null));
        assertEquals(OrderStatus.CANCELLED, order.getStatus());
    }

    @Test
    void failedOrderCannotBeConfirmed() {
        var order = new OrderEntity(UUID.randomUUID(), UUID.randomUUID(), "sku-1", 1,
                new BigDecimal("12.00"), "USD", false);
        assertTrue(order.transition(OrderStatus.FAILED, "Insufficient stock"));
        assertFalse(order.transition(OrderStatus.CONFIRMED, null));
        assertEquals("Insufficient stock", order.getFailureReason());
    }

    @Test
    void acceptsPaymentResultThatArrivesBeforeInventoryStatusEvent() {
        var order = new OrderEntity(UUID.randomUUID(), UUID.randomUUID(), "sku-1", 1,
                new BigDecimal("12.00"), "USD", false);
        assertTrue(order.transition(OrderStatus.CONFIRMED, null));
        assertFalse(order.transition(OrderStatus.PAYMENT_PENDING, null));
        assertEquals(OrderStatus.CONFIRMED, order.getStatus());
    }
}
