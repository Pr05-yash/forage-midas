package com.jpmc.midascore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RestTemplate restTemplate; // 1. RestTemplate inject kiya

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        // 2. Incentive API Call
        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive", 
                transaction, 
                Incentive.class
        );

        // 3. Incentive set karein
        transaction.setIncentive(incentive.getAmount());

        // 4. Balance update logic (Sender & Receiver)
        var sender = userRepository.findByName(transaction.getSenderName());
        var recipient = userRepository.findByName(transaction.getRecipientName());

        // Sender ka balance kam karein
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        
        // Recipient ka balance: transaction amount + incentive amount
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + transaction.getIncentive());

        // Save changes
        userRepository.save(sender);
        userRepository.save(recipient);
    }
}
