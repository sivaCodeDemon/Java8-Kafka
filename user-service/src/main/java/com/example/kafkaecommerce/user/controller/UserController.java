package com.example.kafkaecommerce.user.controller;

import com.example.kafkaecommerce.user.dto.UserRequest;
import com.example.kafkaecommerce.user.entity.UserEntity;
import com.example.kafkaecommerce.user.service.UserService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/users")
public class UserController {
    private final UserService service;

    public UserController(UserService service) { this.service = service; }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public UserResponse create(@Valid @RequestBody UserRequest request,
                               @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        return UserResponse.from(service.create(request, correlationId(correlationId)));
    }

    @GetMapping("/{id}")
    public UserResponse get(@PathVariable UUID id) { return UserResponse.from(service.get(id)); }

    @PutMapping("/{id}")
    public UserResponse update(@PathVariable UUID id, @Valid @RequestBody UserRequest request,
                               @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        return UserResponse.from(service.update(id, request, correlationId(correlationId)));
    }

    private String correlationId(String value) { return value == null || value.isBlank() ? UUID.randomUUID().toString() : value; }

    public record UserResponse(UUID id, String email, String fullName, Instant createdAt) {
        static UserResponse from(UserEntity user) {
            return new UserResponse(user.getId(), user.getEmail(), user.getFullName(), user.getCreatedAt());
        }
    }
}
