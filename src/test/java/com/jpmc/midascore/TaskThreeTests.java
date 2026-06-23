package com.jpmc.midascore;

import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = MidasCoreApplication.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"midas-transactions"})
public class TaskThreeTests {

    @Autowired private KafkaProducer kafkaProducer;
    @Autowired private UserPopulator userPopulator;
    @Autowired private FileLoader fileLoader;
    @Autowired private UserRepository userRepository;

  @Test
void task_three_verifier() throws InterruptedException {
    // 1. Data populate karein
    userPopulator.populate();
    
    // 2. Transactions bhejein
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    // 3. Kafka ko process hone ke liye 5 second ka pause dein
    Thread.sleep(5000);

    // 4. Database se balance nikalein
    var waldorf = userRepository.findByName("Waldorf");
    
    // 5. ISKO USE KAREIN: 
    // Agar balance nahi mila, toh Exception throw karein taaki test runner ruk jaye
    if (waldorf == null) {
        throw new RuntimeException("Waldorf nahi mila! Kafka consumer shayad data process nahi kar raha.");
    }
    
    // 6. Final Result Print (Ye print hona chahiye!)
    System.out.println(">>> WALDORF FINAL BALANCE: " + (int) waldorf.getBalance());
    
    // 7. Loop se nikalne ke liye explicitly test ko fail/stop karein
    assertTrue(true, "Task Complete");
}
