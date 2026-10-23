package com.example.kafkaecommerce.payment.controller;

import com.example.kafkaecommerce.events.Topics;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/kafka-demo")
public class KafkaTransactionDemoController {
    private final KafkaTemplate<String, String> transactionalTemplate;

    public KafkaTransactionDemoController(
            @Qualifier("transactionalDemoKafkaTemplate") KafkaTemplate<String, String> transactionalTemplate) {
        this.transactionalTemplate = transactionalTemplate;
    }

    @PostMapping("/transaction")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public TransactionResult publishAtomically(@Valid @RequestBody TransactionRequest request) {
        int published = transactionalTemplate.executeInTransaction(operations -> {
            try {
                operations.send(Topics.TRANSACTION_DEMO, request.key(), request.firstMessage()).get();
                operations.send(Topics.TRANSACTION_DEMO, request.key(), request.secondMessage()).get();
                return 2;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Kafka transaction was interrupted", e);
            } catch (ExecutionException e) {
                throw new IllegalStateException("Kafka transaction could not publish", e);
            }
        });
        return new TransactionResult(published, Topics.TRANSACTION_DEMO);
    }

    public record TransactionRequest(@NotBlank @Size(max = 100) String key,
                                     @NotBlank @Size(max = 500) String firstMessage,
                                     @NotBlank @Size(max = 500) String secondMessage) {
    }

    public record TransactionResult(int publishedMessages, String topic) {
    }
}
