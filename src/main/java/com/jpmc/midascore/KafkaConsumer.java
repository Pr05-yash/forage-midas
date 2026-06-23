package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(String transaction) { // 'Object' ki jagah 'String' use karein
        // 1. Transaction string ko parse karein (aapke data format ke hisaab se)
        // Example: "sender,recipient,amount"
        String[] parts = transaction.split(",");
        String senderName = parts[0];
        String recipientName = parts[1];
        float amount = Float.parseFloat(parts[2]);

        // 2. Database se users ko find karein
        UserRecord sender = userRepository.findByName(senderName);
        UserRecord recipient = userRepository.findByName(recipientName);

        // 3. Validation aur Update Logic
        if (sender != null && recipient != null && sender.getBalance() >= amount) {
            // Balance adjust karein
            sender.setBalance(sender.getBalance() - amount);
            recipient.setBalance(recipient.getBalance() + amount);

            // Database mein save karein
            userRepository.save(sender);
            userRepository.save(recipient);
            
            // Transaction record save karein
            transactionRepository.save(new TransactionRecord(sender, recipient, amount));
            
            System.out.println("SUCCESS: Transaction processed for " + senderName);
        } else {
            System.out.println("DISCARDED: Invalid transaction from " + senderName);
        }
    }
}
