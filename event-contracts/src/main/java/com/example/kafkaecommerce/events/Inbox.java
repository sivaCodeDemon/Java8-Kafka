package com.example.kafkaecommerce.events;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.UUID;

public class Inbox {
    private final JdbcTemplate jdbcTemplate;

    public Inbox(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean firstDelivery(String consumer, UUID eventId) {
        return jdbcTemplate.update("""
                INSERT INTO processed_events (consumer_name, event_id) VALUES (?, ?)
                ON CONFLICT (consumer_name, event_id) DO NOTHING
                """, consumer, eventId) == 1;
    }
}
