package com.example.kafkaecommerce.order.controller;

import com.example.kafkaecommerce.order.dto.CreateOrderRequest;
import com.example.kafkaecommerce.order.entity.OrderEntity;
import com.example.kafkaecommerce.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderService service;

    public OrderController(OrderService service) { this.service = service; }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderResponse create(@Valid @RequestBody CreateOrderRequest request,
                                @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        return OrderResponse.from(service.create(request, correlationId(correlationId)));
    }

    @GetMapping("/{id}")
    public OrderResponse get(@PathVariable UUID id) { return OrderResponse.from(service.get(id)); }

    @PostMapping("/{id}/cancel")
    public OrderResponse cancel(@PathVariable UUID id,
                                @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        return OrderResponse.from(service.cancel(id, correlationId(correlationId)));
    }

    private String correlationId(String value) { return value == null || value.isBlank() ? UUID.randomUUID().toString() : value; }

    public record OrderResponse(UUID id, UUID userId, String productId, int quantity, BigDecimal amount,
                                String currency, String status, String failureReason, Instant createdAt,
                                Instant updatedAt) {
        static OrderResponse from(OrderEntity order) {
            return new OrderResponse(order.getId(), order.getUserId(), order.getProductId(), order.getQuantity(),
                    order.getAmount(), order.getCurrency(), order.getStatus().name(), order.getFailureReason(),
                    order.getCreatedAt(), order.getUpdatedAt());
        }
    }
}
