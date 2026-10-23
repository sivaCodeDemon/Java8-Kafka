package com.example.kafkaecommerce.order.dto;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderRequest(@NotNull UUID userId,
                                @NotBlank @Size(max = 100) String productId,
                                @Min(1) @Max(10000) int quantity,
                                @NotNull @DecimalMin(value = "0.01") @Digits(integer = 17, fraction = 2) BigDecimal amount,
                                @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
                                boolean simulatePaymentFailure) {
}
