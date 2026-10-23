package com.example.kafkaecommerce.events;

import java.time.Instant;
import java.util.UUID;

public interface DomainEvent {
    UUID eventId();

    String correlationId();

    Instant occurredAt();
}
