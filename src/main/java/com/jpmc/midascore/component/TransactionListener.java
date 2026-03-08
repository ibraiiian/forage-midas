package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.PrintWriter;

@Component
public class TransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
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

                // Add to recipient and save
                recipient.setBalance(recipient.getBalance() + transaction.getAmount());
                databaseConduit.save(recipient);

                // Save transaction record
                com.jpmc.midascore.entity.TransactionRecord transactionRecord = new com.jpmc.midascore.entity.TransactionRecord(
                        sender, recipient, transaction.getAmount());
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
