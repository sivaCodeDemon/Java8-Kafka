package com.example.kafkaecommerce.inventory.controller;

import com.example.kafkaecommerce.inventory.dto.StockRequest;
import com.example.kafkaecommerce.inventory.service.InventoryService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/inventory")
public class InventoryController {
    private final InventoryService service;

    public InventoryController(InventoryService service) { this.service = service; }

    @GetMapping("/{productId}")
    public InventoryService.StockView get(@PathVariable String productId) {
        var stock = service.get(productId);
        if (stock == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Product stock not found");
        return stock;
    }

    @PutMapping("/{productId}/stock")
    public InventoryService.StockView set(@PathVariable String productId,
                                          @Valid @RequestBody StockRequest request) {
        if (productId.isBlank() || productId.length() > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid productId");
        }
        return service.set(productId, request.availableStock());
    }
}
