package com.example.kafkaecommerce.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

public class OutboxWriter {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxWriter(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public void append(String topic, String key, DomainEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            jdbcTemplate.update("""
                    INSERT INTO outbox_events (event_id, topic, event_key, payload)
                    VALUES (?, ?, ?, CAST(? AS jsonb))
                    """, event.eventId(), topic, key, payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize event " + event.eventId(), e);
        }
    }
}
