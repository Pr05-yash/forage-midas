package com.jpmc.midascore;

import com.jpmc.midascore.entity.Transaction; // Import check karein
import com.jpmc.midascore.entity.Incentive;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class KafkaConsumer {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RestTemplate restTemplate;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        // 1. Incentive API se amount mangwayein
        Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive", 
                transaction, 
                Incentive.class
        );

        // 2. Transaction object mein incentive set karein
        double incentiveAmount = (incentive != null) ? incentive.getAmount() : 0.0;
        transaction.setIncentive(incentiveAmount);

        // 3. User entities fetch karein
        var sender = userRepository.findByName(transaction.getSenderName());
        var recipient = userRepository.findByName(transaction.getRecipientName());

        // 4. Logic update: 
        // Sender se sirf transaction amount katega
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        
        // Recipient ko transaction amount + incentive milega
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // 5. Database mein save karein
        userRepository.save(sender);
        userRepository.save(recipient);
        
        // Debug ke liye console par dekhein
        System.out.println("Processed: " + transaction.getSenderName() + " -> " + transaction.getRecipientName() + 
                           " | Incentive: " + incentiveAmount);
    }
}
