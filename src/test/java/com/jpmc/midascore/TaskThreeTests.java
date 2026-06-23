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
@EmbeddedKafka(
    partitions = 1, 
    topics = {"midas-transactions"}, // Ye line add karein
    brokerProperties = { 
        "listeners=PLAINTEXT://localhost:9092", 
        "port=9092"
    }
)
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
    // 1. Data populate karein
    userPopulator.populate();
    
    // 2. Transactions load karke Kafka pe bhejein
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    // 3. Kafka process hone ka wait (30 seconds)
    System.out.println("Processing transactions...");
  // Kafka process hone ka wait
        Thread.sleep(10000); 

        // Balance fetch aur print
        var waldorf = userRepository.findByName("Waldorf");
        System.out.println(">>> WALDORF FINAL BALANCE: " + (int) Math.floor(waldorf.getBalance()));
    if (waldorf != null) {
        System.out.println("WALDORF FINAL BALANCE IS: " + waldorf.getBalance());
    } else {
        System.out.println("ERROR: WALDORF DATABASE MEIN NAHI MILA!");
    }
    System.out.println("##########################################################\n\n");
}
