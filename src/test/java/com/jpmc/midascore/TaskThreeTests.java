package com.jpmc.midascore;

import org.junit.jupiter.api.BeforeEach; // Naya import
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(classes = MidasCoreApplication.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"midas-transactions"})
public class TaskThreeTests {

    @BeforeEach
    void setup() {
        System.out.println(">>> [DEBUG] SETUP CHAL GAYA HAI");
    }

 @Test
void task_three_verifier() throws InterruptedException {
    userPopulator.populate();
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    // Kafka ko process karne ka time dein
    Thread.sleep(5000); 

    // RESULT PRINT KAREIN (Yahi aapko chahiye)
    var waldorf = userRepository.findByName("Waldorf");
    System.out.println("\n\n################################################");
    System.out.println("WALDORF BALANCE: " + waldorf.getBalance());
    System.out.println("################################################\n\n");
}
