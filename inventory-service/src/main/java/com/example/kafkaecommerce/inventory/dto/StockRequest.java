package com.example.kafkaecommerce.inventory.dto;

import jakarta.validation.constraints.Min;

public record StockRequest(@Min(0) int availableStock) {
}
