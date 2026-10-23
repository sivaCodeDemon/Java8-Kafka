package com.example.kafkaecommerce.order.service;

import com.example.kafkaecommerce.events.*;
import com.example.kafkaecommerce.order.dto.CreateOrderRequest;
import com.example.kafkaecommerce.order.entity.OrderEntity;
import com.example.kafkaecommerce.order.entity.OrderStatus;
import com.example.kafkaecommerce.order.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {
    private final OrderRepository orders;
    private final OutboxWriter outbox;
    private final Inbox inbox;
    private final ObjectMapper mapper;
    private final RestClient usersClient;

    public OrderService(OrderRepository orders, OutboxWriter outbox, Inbox inbox, ObjectMapper mapper,
                        RestClient.Builder restClientBuilder,
                        @Value("${app.user-service-url:http://localhost:8081}") String userServiceUrl) {
        this.orders = orders;
        this.outbox = outbox;
        this.inbox = inbox;
        this.mapper = mapper;
        this.usersClient = restClientBuilder.baseUrl(userServiceUrl).build();
    }

    @Transactional
    public OrderEntity create(CreateOrderRequest request, String correlationId) {
        try {
            usersClient.get().uri("/users/{id}", request.userId()).retrieve().toBodilessEntity();
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().value() == 404) {
                throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "Unknown user");
            }
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "User service is unavailable", e);
        } catch (org.springframework.web.client.ResourceAccessException e) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "User service is unavailable", e);
        }
        UUID id = UUID.randomUUID();
        OrderEntity order = orders.save(new OrderEntity(id, request.userId(), request.productId(),
                request.quantity(), request.amount(), request.currency(), request.simulatePaymentFailure()));
        outbox.append(Topics.ORDER_CREATED, id.toString(), new Events.OrderCreated(
                UUID.randomUUID(), correlationId, Instant.now(), id, request.userId(), request.productId(),
                request.quantity(), request.amount(), request.currency(), request.simulatePaymentFailure()));
        return order;
    }

    @Transactional(readOnly = true)
    public OrderEntity get(UUID id) {
        return orders.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found"));
    }

    @Transactional
    public OrderEntity cancel(UUID id, String correlationId) {
        OrderEntity order = orders.findByIdForUpdate(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found"));
        if (order.getStatus() == OrderStatus.CANCELLED) return order;
        if (order.getStatus() == OrderStatus.FAILED) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Failed orders cannot be cancelled");
        }
        if (order.transition(OrderStatus.CANCELLED, "Cancelled by customer")) {
            outbox.append(Topics.ORDER_CANCELLED, id.toString(),
                    new Events.OrderCancelled(UUID.randomUUID(), correlationId, Instant.now(), id));
            publishStatus(order, correlationId, "Cancelled by customer");
        }
        return order;
    }

    @Transactional
    public void onInventoryReserved(String payload) throws Exception {
        Events.InventoryReserved event = mapper.readValue(payload, Events.InventoryReserved.class);
        if (!inbox.firstDelivery("order-inventory-reserved", event.eventId())) return;
        OrderEntity order = orders.findByIdForUpdate(event.orderId()).orElse(null);
        if (order == null) return;
        if (order.transition(OrderStatus.INVENTORY_RESERVED, null)) {
            order.transition(OrderStatus.PAYMENT_PENDING, null);
            publishStatus(order, event.correlationId(), null);
        }
    }

    @Transactional
    public void onInventoryRejected(String payload) throws Exception {
        Events.InventoryRejected event = mapper.readValue(payload, Events.InventoryRejected.class);
        if (!inbox.firstDelivery("order-inventory-rejected", event.eventId())) return;
        OrderEntity order = orders.findByIdForUpdate(event.orderId()).orElse(null);
        if (order != null && order.transition(OrderStatus.FAILED, event.reason())) {
            publishStatus(order, event.correlationId(), event.reason());
        }
    }

    @Transactional
    public void onPaymentSucceeded(String payload) throws Exception {
        Events.PaymentSucceeded event = mapper.readValue(payload, Events.PaymentSucceeded.class);
        if (!inbox.firstDelivery("order-payment-succeeded", event.eventId())) return;
        OrderEntity order = orders.findByIdForUpdate(event.orderId()).orElse(null);
        if (order != null && order.transition(OrderStatus.CONFIRMED, null)) {
            publishStatus(order, event.correlationId(), null);
        }
    }

    @Transactional
    public void onPaymentFailed(String payload) throws Exception {
        Events.PaymentFailed event = mapper.readValue(payload, Events.PaymentFailed.class);
        if (!inbox.firstDelivery("order-payment-failed", event.eventId())) return;
        OrderEntity order = orders.findByIdForUpdate(event.orderId()).orElse(null);
        if (order != null && order.transition(OrderStatus.FAILED, event.reason())) {
            publishStatus(order, event.correlationId(), event.reason());
        }
    }

    private void publishStatus(OrderEntity order, String correlationId, String reason) {
        outbox.append(Topics.ORDER_STATUS, order.getId().toString(),
                new Events.OrderStatusChanged(UUID.randomUUID(), correlationId, Instant.now(),
                        order.getId(), order.getStatus().name(), reason));
    }
}
