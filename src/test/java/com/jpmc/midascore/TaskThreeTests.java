package com.jpmc.midascore;

import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(classes = MidasCoreApplication.class) // Ye change karein
@DirtiesContext
@EmbeddedKafka(
    partitions = 1, 
    topics = {"midas-transactions"},
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
    System.out.println(">>> [DEBUG] TEST START HO GAYA HAI!"); // Check karne ke liye
    
    userPopulator.populate();
    Thread.sleep(2000); 
    
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    System.out.println(">>> [DEBUG] MESSAGES BHEJ DIYE HAIN, AB WAIT KAR RAHE HAIN...");
    Thread.sleep(10000); // 10 second wait

    var waldorf = userRepository.findByName("Waldorf");
    
    if (waldorf != null) {
        System.out.println(">>> WALDORF FINAL BALANCE: " + (int) Math.floor(waldorf.getBalance()));
    } else {
        System.out.println(">>> ERROR: WALDORF NAHI MILA!");
    }
}
