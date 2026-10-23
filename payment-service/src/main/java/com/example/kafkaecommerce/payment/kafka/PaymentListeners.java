package com.example.kafkaecommerce.payment.kafka;

import com.example.kafkaecommerce.events.Topics;
import com.example.kafkaecommerce.payment.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentListeners {
    private final PaymentService service;

    public PaymentListeners(PaymentService service) { this.service = service; }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "payment-service")
    public void inventoryReserved(String payload) throws Exception { service.onInventoryReserved(payload); }

    @KafkaListener(topics = Topics.ORDER_CANCELLED, groupId = "payment-service")
    public void orderCancelled(String payload) throws Exception { service.onOrderCancelled(payload); }
}
