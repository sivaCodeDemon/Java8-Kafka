package com.example.kafkaecommerce.events;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

public class OutboxPublisher {
    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        List<OutboxRow> rows = jdbcTemplate.query("""
                SELECT event_id, topic, event_key, payload::text
                FROM outbox_events WHERE published_at IS NULL
                ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED
                """, (rs, rowNum) -> new OutboxRow(
                rs.getObject("event_id", UUID.class),
                rs.getString("topic"),
                rs.getString("event_key"),
                rs.getString("payload")));
        for (OutboxRow row : rows) {
            try {
                kafkaTemplate.send(row.topic(), row.key(), row.payload()).get();
                jdbcTemplate.update("UPDATE outbox_events SET published_at = now() WHERE event_id = ?",
                        row.eventId());
            } catch (Exception e) {
                throw new IllegalStateException("Outbox publish failed for " + row.eventId(), e);
            }
        }
    }

    private record OutboxRow(UUID eventId, String topic, String key, String payload) {
    }
}
