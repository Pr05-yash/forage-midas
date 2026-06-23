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
    userPopulator.populate();
    
    // Kafka ko message process karne ke liye time dein
    Thread.sleep(5000); 
    
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    // Yahan main wait 10 seconds ka hai, isse zyada nahi
    Thread.sleep(10000); 

    var waldorf = userRepository.findByName("Waldorf");
    
    // PRINT RESULT
    if (waldorf != null) {
        System.out.println("WALDORF FINAL BALANCE: " + waldorf.getBalance());
    } else {
        System.out.println("WALDORF NAHI MILA!");
    }
}
