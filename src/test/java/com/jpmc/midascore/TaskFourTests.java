package com.jpmc.midascore;

import com.jpmc.midascore.entity.User;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"})
public class TaskFourTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired private KafkaProducer kafkaProducer;
    @Autowired private UserPopulator userPopulator;
    @Autowired private FileLoader fileLoader;
    @Autowired private UserRepository userRepository;

    @Test
    void task_four_verifier() throws InterruptedException {
        // 1. Data populate karein
        userPopulator.populate();
        
        // 2. Transactions load karke bhejein
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // 3. Kafka ko processing ke liye time dein (Incentive API call include hai)
        Thread.sleep(10000); 

        // 4. Wilbur ka balance fetch karein
        User wilbur = userRepository.findByName("Wilbur");
        
        if (wilbur != null) {
            logger.info("**********************************************************");
            logger.info("FINAL BALANCE OF WILBUR: " + (int) wilbur.getBalance());
            logger.info("**********************************************************");
        } else {
            logger.error("Wilbur nahi mila!");
        }
    }
}
