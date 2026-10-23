package com.example.kafkaecommerce.user.service;

import com.example.kafkaecommerce.events.Events;
import com.example.kafkaecommerce.events.OutboxWriter;
import com.example.kafkaecommerce.events.Topics;
import com.example.kafkaecommerce.user.dto.UserRequest;
import com.example.kafkaecommerce.user.entity.UserEntity;
import com.example.kafkaecommerce.user.repository.UserRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

@Service
public class UserService {
    private final UserRepository users;
    private final OutboxWriter outbox;

    public UserService(UserRepository users, OutboxWriter outbox) {
        this.users = users;
        this.outbox = outbox;
    }

    @Transactional
    public UserEntity create(UserRequest request, String correlationId) {
        UserEntity user = users.save(new UserEntity(UUID.randomUUID(), request.email(), request.fullName()));
        outbox.append(Topics.USER_CREATED, user.getId().toString(),
                new Events.UserCreated(UUID.randomUUID(), correlationId, Instant.now(),
                        user.getId(), user.getEmail(), user.getFullName()));
        return user;
    }

    @Transactional
    public UserEntity update(UUID id, UserRequest request, String correlationId) {
        UserEntity user = users.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found"));
        user.update(request.email(), request.fullName());
        outbox.append(Topics.USER_UPDATED, user.getId().toString(),
                new Events.UserUpdated(UUID.randomUUID(), correlationId, Instant.now(),
                        user.getId(), user.getEmail(), user.getFullName()));
        return user;
    }

    @Transactional(readOnly = true)
    public UserEntity get(UUID id) {
        return users.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found"));
    }
}
