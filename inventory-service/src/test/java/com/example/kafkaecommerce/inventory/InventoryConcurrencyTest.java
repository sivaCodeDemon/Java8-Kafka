package com.example.kafkaecommerce.inventory;

import com.example.kafkaecommerce.events.*;
import com.example.kafkaecommerce.inventory.service.InventoryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers(disabledWithoutDocker = true)
class InventoryConcurrencyTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16.4");

    @BeforeEach
    void clearTables() {
        var dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        dataSource.setDriverClassName("org.postgresql.Driver");
        new JdbcTemplate(dataSource).execute("""
                DROP TABLE IF EXISTS outbox_events, processed_events, inventory_reservations,
                cancelled_orders, product_stock CASCADE
                """);
    }

    @Test
    void concurrentReservationsDoNotOversell() throws Exception {
        var dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        dataSource.setDriverClassName("org.postgresql.Driver");
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE product_stock(product_id VARCHAR(100) PRIMARY KEY, available_stock INTEGER NOT NULL)");
        jdbc.execute("CREATE TABLE inventory_reservations(order_id UUID PRIMARY KEY, product_id VARCHAR(100), quantity INTEGER, status VARCHAR(16))");
        jdbc.execute("CREATE TABLE cancelled_orders(order_id UUID PRIMARY KEY)");
        jdbc.execute("CREATE TABLE processed_events(consumer_name VARCHAR(150), event_id UUID, processed_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(consumer_name,event_id))");
        jdbc.execute("CREATE TABLE outbox_events(event_id UUID PRIMARY KEY, topic VARCHAR(200), event_key VARCHAR(200), payload JSONB, created_at TIMESTAMPTZ DEFAULT now(), published_at TIMESTAMPTZ)");
        jdbc.update("INSERT INTO product_stock VALUES ('one-left', 1)");
        var mapper = new ObjectMapper().findAndRegisterModules();
        var service = new InventoryService(jdbc, new Inbox(jdbc), new OutboxWriter(jdbc, mapper), mapper);
        var transactions = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> first = pool.submit(() -> reserve(transactions, service, start, orderCreated("one-left")));
            Future<?> second = pool.submit(() -> reserve(transactions, service, start, orderCreated("one-left")));
            start.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(0, jdbc.queryForObject("SELECT available_stock FROM product_stock WHERE product_id='one-left'", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM inventory_reservations", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE topic = ?", Integer.class, Topics.INVENTORY_RESERVED));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE topic = ?", Integer.class, Topics.INVENTORY_REJECTED));
    }

    @Test
    void duplicateDeliveryAndCompensationDoNotReserveOrReleaseTwice() throws Exception {
        var dataSource = new DriverManagerDataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        dataSource.setDriverClassName("org.postgresql.Driver");
        var jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE product_stock(product_id VARCHAR(100) PRIMARY KEY, available_stock INTEGER NOT NULL)");
        jdbc.execute("CREATE TABLE inventory_reservations(order_id UUID PRIMARY KEY, product_id VARCHAR(100), quantity INTEGER, status VARCHAR(16))");
        jdbc.execute("CREATE TABLE cancelled_orders(order_id UUID PRIMARY KEY)");
        jdbc.execute("CREATE TABLE processed_events(consumer_name VARCHAR(150), event_id UUID, processed_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(consumer_name,event_id))");
        jdbc.execute("CREATE TABLE outbox_events(event_id UUID PRIMARY KEY, topic VARCHAR(200), event_key VARCHAR(200), payload JSONB, created_at TIMESTAMPTZ DEFAULT now(), published_at TIMESTAMPTZ)");
        jdbc.update("INSERT INTO product_stock VALUES ('twice', 3)");
        var mapper = new ObjectMapper().findAndRegisterModules();
        var service = new InventoryService(jdbc, new Inbox(jdbc), new OutboxWriter(jdbc, mapper), mapper);
        var tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        UUID orderId = UUID.randomUUID();
        String create = mapper.writeValueAsString(new Events.OrderCreated(UUID.randomUUID(), "corr",
                Instant.now(), orderId, UUID.randomUUID(), "twice", 1,
                new BigDecimal("10.00"), "USD", false));
        tx.executeWithoutResult(status -> invoke(() -> service.onOrderCreated(create)));
        tx.executeWithoutResult(status -> invoke(() -> service.onOrderCreated(create)));
        assertEquals(2, jdbc.queryForObject("SELECT available_stock FROM product_stock WHERE product_id='twice'", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE topic = ?", Integer.class, Topics.INVENTORY_RESERVED));

        String failure = mapper.writeValueAsString(new Events.PaymentFailed(UUID.randomUUID(), "corr",
                Instant.now(), orderId, UUID.randomUUID(), "simulated decline"));
        tx.executeWithoutResult(status -> invoke(() -> service.onPaymentFailed(failure)));
        tx.executeWithoutResult(status -> invoke(() -> service.onPaymentFailed(failure)));
        assertEquals(3, jdbc.queryForObject("SELECT available_stock FROM product_stock WHERE product_id='twice'", Integer.class));
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM outbox_events WHERE topic = ?", Integer.class, Topics.INVENTORY_RELEASED));
    }

    private static void reserve(TransactionTemplate tx, InventoryService service, CountDownLatch start, String payload) {
        try {
            start.await();
            tx.executeWithoutResult(status -> {
                try {
                    service.onOrderCreated(payload);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private static String orderCreated(String product) {
        try {
            return new ObjectMapper().findAndRegisterModules().writeValueAsString(new Events.OrderCreated(UUID.randomUUID(),
                    UUID.randomUUID().toString(), Instant.now(), UUID.randomUUID(), UUID.randomUUID(),
                    product, 1, new BigDecimal("10.00"), "USD", false));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void invoke(CheckedAction action) {
        try {
            action.run();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws Exception;
    }
}
