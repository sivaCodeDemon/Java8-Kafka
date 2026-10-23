package com.example.kafkaecommerce.inventory.kafka;

import com.example.kafkaecommerce.events.Topics;
import com.example.kafkaecommerce.inventory.service.InventoryService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryListeners {
    private final InventoryService service;

    public InventoryListeners(InventoryService service) { this.service = service; }

    @KafkaListener(topics = Topics.ORDER_CREATED, groupId = "inventory-service")
    public void orderCreated(String payload) throws Exception { service.onOrderCreated(payload); }

    @KafkaListener(topics = Topics.ORDER_CANCELLED, groupId = "inventory-service")
    public void orderCancelled(String payload) throws Exception { service.onOrderCancelled(payload); }

    @KafkaListener(topics = Topics.PAYMENT_FAILED, groupId = "inventory-service")
    public void paymentFailed(String payload) throws Exception { service.onPaymentFailed(payload); }
}
