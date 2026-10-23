package com.example.kafkaecommerce.payment.controller;

import com.example.kafkaecommerce.payment.entity.PaymentEntity;
import com.example.kafkaecommerce.payment.service.PaymentService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/payments")
public class PaymentController {
    private final PaymentService service;

    public PaymentController(PaymentService service) { this.service = service; }

    @GetMapping("/{paymentId}")
    public PaymentResponse get(@PathVariable UUID paymentId) {
        return PaymentResponse.from(service.get(paymentId));
    }

    @GetMapping("/by-order/{orderId}")
    public PaymentResponse getByOrder(@PathVariable UUID orderId) {
        return PaymentResponse.from(service.getByOrderId(orderId));
    }

    @PostMapping("/{paymentId}/refund")
    @ResponseStatus(HttpStatus.OK)
    public PaymentResponse refund(@PathVariable UUID paymentId,
                                  @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        String value = correlationId == null || correlationId.isBlank() ? UUID.randomUUID().toString() : correlationId;
        return PaymentResponse.from(service.refund(paymentId, value));
    }

    public record PaymentResponse(UUID id, UUID orderId, BigDecimal amount, String currency, String status,
                                  String failureReason, Instant createdAt) {
        static PaymentResponse from(PaymentEntity payment) {
            return new PaymentResponse(payment.getId(), payment.getOrderId(), payment.getAmount(),
                    payment.getCurrency(), payment.getStatus().name(), payment.getFailureReason(),
                    payment.getCreatedAt());
        }
    }
}
