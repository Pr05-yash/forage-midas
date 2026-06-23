package com.jpmc.midascore;

import com.jpmc.midascore.component.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        System.out.println("Received transaction: " + transaction);
    }
}
