package com.example.kafkaecommerce.payment.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.UUID;

@Configuration
public class KafkaTransactionDemoConfiguration {
    @Bean
    public ProducerFactory<String, String> demoTransactionProducerFactory(
            @Qualifier("kafkaProducerFactory")
            ProducerFactory<String, String> producerFactory) {
        var transactionalFactory = new DefaultKafkaProducerFactory<String, String>(
                new HashMap<>(producerFactory.getConfigurationProperties()));
        transactionalFactory.setTransactionIdPrefix("payment-demo-" + UUID.randomUUID() + "-");
        return transactionalFactory;
    }

    @Bean
    @Qualifier("transactionalDemoKafkaTemplate")
    public KafkaTemplate<String, String> transactionalDemoKafkaTemplate(
            @Qualifier("demoTransactionProducerFactory")
            ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
