package com.jpmc.midascore;

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
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {
    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;
    @Autowired
    private UserPopulator userPopulator;
    @Autowired
    private FileLoader fileLoader;
    @Autowired
    private UserRepository userRepository;

    @Test
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // Kafka process hone ka wait
       Thread.sleep(10000); 

var allUsers = userRepository.findAll();
logger.info("DEBUG: Database mein kitne users hain: " + allUsers.size());
for (var user : allUsers) {
    logger.info("DEBUG: User mila: " + user.getName() + " | Balance: " + user.getBalance());
}

var waldorf = userRepository.findByName("Waldorf");
if (waldorf == null) {
    logger.info("ERROR: Waldorf database mein nahi mila!");
} else {
    logger.info("**********************************************************");
    logger.info("WALDORF FINAL BALANCE: " + (int) Math.floor(waldorf.getBalance()));
    logger.info("**********************************************************");
}
        

        var waldorf = userRepository.findByName("Waldorf");
        if (waldorf != null) {
            logger.info("**********************************************************");
            logger.info("WALDORF FINAL BALANCE: " + (int) Math.floor(waldorf.getBalance()));
            logger.info("**********************************************************");
        }
    }
}
