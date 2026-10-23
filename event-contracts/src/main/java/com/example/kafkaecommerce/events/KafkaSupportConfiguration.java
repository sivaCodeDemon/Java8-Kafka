package com.example.kafkaecommerce.events;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaSupportConfiguration {
    private static final String[] TOPICS = {
            Topics.USER_CREATED, Topics.USER_UPDATED, Topics.ORDER_CREATED, Topics.ORDER_CANCELLED,
            Topics.ORDER_STATUS, Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED,
            Topics.INVENTORY_RELEASED, Topics.PAYMENT_SUCCEEDED, Topics.PAYMENT_FAILED,
            Topics.PAYMENT_REFUNDED, Topics.PAYMENT_COUNTS, Topics.TRANSACTION_DEMO
    };

    @Bean
    public OutboxWriter outboxWriter(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate,
                                     com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
        return new OutboxWriter(jdbcTemplate, objectMapper);
    }

    @Bean
    public OutboxPublisher outboxPublisher(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate,
                                           KafkaTemplate<String, String> kafkaTemplate) {
        return new OutboxPublisher(jdbcTemplate, kafkaTemplate);
    }

    @Bean
    public Inbox inbox(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
        return new Inbox(jdbcTemplate);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate, (record, exception) -> new TopicPartition(record.topic() + ".dlt", record.partition()));
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(5000L);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean
    public org.springframework.kafka.config.KafkaListenerContainerFactory<
            org.springframework.kafka.listener.ConcurrentMessageListenerContainer<String, String>>
    kafkaListenerContainerFactory(
            org.springframework.kafka.core.ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        var factory = new org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.RECORD);
        return factory;
    }

    @Bean
    public org.springframework.kafka.config.KafkaListenerContainerFactory<
            org.springframework.kafka.listener.ConcurrentMessageListenerContainer<String, String>>
    batchKafkaListenerContainerFactory(
            org.springframework.kafka.core.ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler) {
        var factory = new org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.setBatchListener(true);
        return factory;
    }

    @Bean
    public org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate(
            org.springframework.kafka.core.ProducerFactory<String, String> producerFactory) {
        return new org.springframework.kafka.core.KafkaTemplate<>(producerFactory);
    }

    @Bean
    public org.springframework.kafka.core.KafkaAdmin.NewTopics kafkaTopics() {
        NewTopic[] topics = new NewTopic[TOPICS.length * 2];
        for (int i = 0; i < TOPICS.length; i++) {
            topics[i * 2] = TopicBuilder.name(TOPICS[i]).partitions(3).replicas(1).build();
            topics[i * 2 + 1] = TopicBuilder.name(TOPICS[i] + ".dlt").partitions(3).replicas(1).build();
        }
        return new org.springframework.kafka.core.KafkaAdmin.NewTopics(topics);
    }
}
