package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    @Transactional
    public void listen(String transaction) {
        System.out.println("@@@ RAW MESSAGE RECEIVED: " + transaction);
        
        // Data parsing (Assuming format: sender,recipient,amount)
        String[] parts = transaction.split(",");
        String senderName = parts[0];
        String recipientName = parts[1];
        float amount = Float.parseFloat(parts[2]);

        UserRecord sender = userRepository.findByName(senderName);
        UserRecord recipient = userRepository.findByName(recipientName);

        if (sender != null && recipient != null && sender.getBalance() >= amount) {
            sender.setBalance(sender.getBalance() - amount);
            recipient.setBalance(recipient.getBalance() + amount);

            userRepository.save(sender);
            userRepository.save(recipient);
            transactionRepository.save(new TransactionRecord(sender, recipient, amount));
            
            System.out.println("SUCCESS: Processed " + senderName + " to " + recipientName);
        } else {
            System.out.println("DISCARDED: " + senderName + " (Invalid or Insufficient Funds)");
        }
    }
}
