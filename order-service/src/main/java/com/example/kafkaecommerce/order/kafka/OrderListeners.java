package com.example.kafkaecommerce.order.kafka;

import com.example.kafkaecommerce.events.Topics;
import com.example.kafkaecommerce.order.service.OrderService;
import com.example.kafkaecommerce.events.Events;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderListeners {
    private final OrderService service;
    private final ObjectMapper mapper;

    public OrderListeners(OrderService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "order-service")
    public void inventoryReserved(String payload) throws Exception { service.onInventoryReserved(payload); }

    @KafkaListener(topics = Topics.INVENTORY_REJECTED, groupId = "order-service")
    public void inventoryRejected(String payload) throws Exception { service.onInventoryRejected(payload); }

    @KafkaListener(topics = Topics.PAYMENT_SUCCEEDED, groupId = "order-service")
    public void paymentSucceeded(String payload) throws Exception { service.onPaymentSucceeded(payload); }

    @KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "order-service")
    public void paymentFailed(String payload) throws Exception { service.onPaymentFailed(payload); }

    @KafkaListener(topics = Topics.ORDER_STATUS, groupId = "order-notification", containerFactory = "batchKafkaListenerContainerFactory")
    public void notifyBatch(java.util.List<String> payloads) throws Exception {
        for (String payload : payloads) {
            Events.OrderStatusChanged event = mapper.readValue(payload, Events.OrderStatusChanged.class);
            if (java.util.Set.of("CONFIRMED", "FAILED", "CANCELLED").contains(event.status())) {
                System.out.printf("Order %s %s (correlation=%s)%n",
                        event.orderId(), event.status(), event.correlationId());
            }
        }
    }
}
