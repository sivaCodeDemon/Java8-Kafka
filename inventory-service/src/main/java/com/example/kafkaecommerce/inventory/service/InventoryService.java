package com.example.kafkaecommerce.inventory.service;

import com.example.kafkaecommerce.events.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class InventoryService {
    private final JdbcTemplate jdbc;
    private final Inbox inbox;
    private final OutboxWriter outbox;
    private final ObjectMapper mapper;

    public InventoryService(JdbcTemplate jdbc, Inbox inbox, OutboxWriter outbox, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.inbox = inbox;
        this.outbox = outbox;
        this.mapper = mapper;
    }

    @Transactional
    public void onOrderCreated(String payload) throws Exception {
        Events.OrderCreated event = mapper.readValue(payload, Events.OrderCreated.class);
        if (!inbox.firstDelivery("inventory-order-created", event.eventId())) return;
        lockOrder(event.orderId());
        Integer cancelled = jdbc.queryForObject("SELECT count(*) FROM cancelled_orders WHERE order_id = ?",
                Integer.class, event.orderId());
        if (cancelled != null && cancelled > 0) return;
        Integer reserved = jdbc.queryForObject("SELECT count(*) FROM inventory_reservations WHERE order_id = ?",
                Integer.class, event.orderId());
        if (reserved != null && reserved > 0) return;
        int updated = jdbc.update("""
                UPDATE product_stock SET available_stock = available_stock - ?
                WHERE product_id = ? AND available_stock >= ?
                """, event.quantity(), event.productId(), event.quantity());
        if (updated == 0) {
            outbox.append(Topics.INVENTORY_REJECTED, event.orderId().toString(),
                    new Events.InventoryRejected(UUID.randomUUID(), event.correlationId(), Instant.now(),
                            event.orderId(), event.productId(), "Insufficient stock"));
            return;
        }
        jdbc.update("""
                INSERT INTO inventory_reservations(order_id, product_id, quantity, status)
                VALUES (?, ?, ?, 'RESERVED')
                """, event.orderId(), event.productId(), event.quantity());
        outbox.append(Topics.INVENTORY_RESERVED, event.orderId().toString(),
                new Events.InventoryReserved(UUID.randomUUID(), event.correlationId(), Instant.now(),
                        event.orderId(), event.productId(), event.quantity(), event.amount(), event.currency(),
                        event.simulatePaymentFailure()));
    }

    @Transactional
    public void onOrderCancelled(String payload) throws Exception {
        Events.OrderCancelled event = mapper.readValue(payload, Events.OrderCancelled.class);
        if (!inbox.firstDelivery("inventory-order-cancelled", event.eventId())) return;
        lockOrder(event.orderId());
        jdbc.update("INSERT INTO cancelled_orders(order_id) VALUES (?) ON CONFLICT DO NOTHING", event.orderId());
        release(event.orderId(), event.correlationId());
    }

    @Transactional
    public void onPaymentFailed(String payload) throws Exception {
        Events.PaymentFailed event = mapper.readValue(payload, Events.PaymentFailed.class);
        if (!inbox.firstDelivery("inventory-payment-failed", event.eventId())) return;
        release(event.orderId(), event.correlationId());
    }

    @Transactional(readOnly = true)
    public StockView get(String productId) {
        return jdbc.query("SELECT product_id, available_stock FROM product_stock WHERE product_id = ?",
                rs -> rs.next() ? new StockView(rs.getString(1), rs.getInt(2)) : null, productId);
    }

    @Transactional
    public StockView set(String productId, int availableStock) {
        jdbc.update("""
                INSERT INTO product_stock(product_id, available_stock) VALUES (?, ?)
                ON CONFLICT (product_id) DO UPDATE SET available_stock = EXCLUDED.available_stock
                """, productId, availableStock);
        return get(productId);
    }

    private void release(UUID orderId, String correlationId) {
        var released = jdbc.query("""
                UPDATE inventory_reservations SET status = 'RELEASED'
                WHERE order_id = ? AND status = 'RESERVED'
                RETURNING product_id, quantity
                """, (rs, rowNum) -> new StockView(rs.getString("product_id"), rs.getInt("quantity")), orderId);
        if (released.isEmpty()) return;
        StockView reservation = released.get(0);
        jdbc.update("UPDATE product_stock SET available_stock = available_stock + ? WHERE product_id = ?",
                reservation.availableStock(), reservation.productId());
        outbox.append(Topics.INVENTORY_RELEASED, orderId.toString(),
                new Events.InventoryReleased(UUID.randomUUID(), correlationId, Instant.now(), orderId,
                        reservation.productId(), reservation.availableStock()));
    }

    private void lockOrder(UUID orderId) {
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtext(?))", Object.class, orderId.toString());
    }

    public record StockView(String productId, int availableStock) {
    }
}
