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
    userPopulator.populate();
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    
    // Kafka Producer ka instance use karein
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }
    
    // Yahan sleep hatayein aur wait condition lagayein
    // Agar waldorf ka balance update nahi hua, toh 10 second baad force stop
    Thread.sleep(8000); 

    var waldorf = userRepository.findByName("Waldorf");
    
    if (waldorf != null) {
        System.out.println("----------------------------------------------");
        System.out.println("WALDORF FINAL BALANCE: " + (int) waldorf.getBalance());
        System.out.println("----------------------------------------------");
    } else {
        System.out.println("!!! WALDORF NOT FOUND IN DATABASE !!!");
    }
}
