package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Transaction; // Trusted package se import
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        // Ye line seedha terminal par amount print karegi
        System.out.println("=======> TRANSACTION AMOUNT IS: " + transaction.getAmount());
    }
}
