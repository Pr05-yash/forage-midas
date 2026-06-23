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
    topics = {"midas-transactions"}, // Topic ka naam yahan hona zaroori hai
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
    System.out.println(">>> TEST START HO GAYA HAI!");
    userPopulator.populate();
    
    // Kafka ko settle hone ka time dein
    Thread.sleep(5000); 
    
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    // Process hone ka wait
    Thread.sleep(10000); 

    var waldorf = userRepository.findByName("Waldorf");
    
    System.out.println("\n\n##########################################################");
    if (waldorf != null) {
        System.out.println(">>> WALDORF FINAL BALANCE: " + (int) Math.floor(waldorf.getBalance()));
    } else {
        System.out.println("ERROR: WALDORF DATABASE MEIN NAHI MILA!");
    }
    System.out.println("##########################################################\n\n");
}
