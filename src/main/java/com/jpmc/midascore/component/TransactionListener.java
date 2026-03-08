package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.jpmc.midascore.foundation.Incentive;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.io.FileWriter;
import java.io.PrintWriter;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final DatabaseConduit databaseConduit;
    private final RestTemplate restTemplate;

    public TransactionListener(DatabaseConduit databaseConduit, RestTemplateBuilder restTemplateBuilder) {
        this.databaseConduit = databaseConduit;
        this.restTemplate = restTemplateBuilder.build();
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = databaseConduit.getUserById(transaction.getSenderId());
        UserRecord recipient = databaseConduit.getUserById(transaction.getRecipientId());

        if (sender != null && recipient != null) {
            if (sender.getBalance() >= transaction.getAmount()) {
                // Deduct from sender and save
                sender.setBalance(sender.getBalance() - transaction.getAmount());
                databaseConduit.save(sender);

                // Call incentive API
                float incentiveAmount = 0.0f;
                try {
                    ResponseEntity<Incentive> response = restTemplate.postForEntity("http://localhost:8080/incentive", transaction, Incentive.class);
                    if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                        incentiveAmount = response.getBody().getAmount();
                        logger.info("Received incentive amount: {}", incentiveAmount);
                    }
                } catch (Exception e) {
                    logger.error("Failed to fetch incentive from API.", e);
                }

                // Add to recipient and save
                recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);
                databaseConduit.save(recipient);

                // Save transaction record
                com.jpmc.midascore.entity.TransactionRecord transactionRecord = new com.jpmc.midascore.entity.TransactionRecord(
                        sender, recipient, transaction.getAmount(), incentiveAmount);
                databaseConduit.save(transactionRecord);

                logger.info("Transaction validated and recorded.");
            } else {
                logger.info("Transaction invalid: Insufficient balance.");
            }
        } else {
            logger.info("Transaction invalid: Sender or recipient not found.");
        }
    }
}
