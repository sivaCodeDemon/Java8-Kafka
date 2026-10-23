package com.example.kafkaecommerce.payment.config;

import com.example.kafkaecommerce.events.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.kafka.streams.kstream.KStream;

@Configuration
public class PaymentStreamConfiguration {
    @Bean
    public KStream<String, String> paymentSuccessCountStream(StreamsBuilder builder) {
        KStream<String, String> successes = builder.stream(
                Topics.PAYMENT_SUCCEEDED, Consumed.with(Serdes.String(), Serdes.String()));
        successes.selectKey((orderId, payload) -> "successful-payments")
                .groupByKey()
                .count(Materialized.as("payment-success-counts"))
                .toStream()
                .to(Topics.PAYMENT_COUNTS, Produced.with(Serdes.String(), Serdes.Long()));
        return successes;
    }
}
