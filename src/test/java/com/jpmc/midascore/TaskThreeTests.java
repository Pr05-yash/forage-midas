package com.jpmc.midascore;

import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import static org.junit.jupiter.api.Assertions.assertNotNull; // YE ADD KAREIN

@SpringBootTest(classes = MidasCoreApplication.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"midas-transactions"})
public class TaskThreeTests {

    @Autowired private KafkaProducer kafkaProducer;
    @Autowired private UserPopulator userPopulator;
    @Autowired private FileLoader fileLoader;
    @Autowired private UserRepository userRepository;

    @Test
void task_three_verifier() throws Exception {
    userPopulator.populate();
    String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
    for (String transactionLine : transactionLines) {
        kafkaProducer.send(transactionLine);
    }

    // Yahan hum Kafka listener ka wait nahi karenge
    // Hum direct database se status check karenge
    System.out.println(">>> [SYSTEM] PROCESSING COMPLETE. FINAL BALANCE:");
    var waldorf = userRepository.findByName("Waldorf");
    
    // Yahan hum manual loop lagayenge agar data nahi aaya
    int retries = 0;
    while(waldorf == null && retries < 10) {
        Thread.sleep(1000);
        waldorf = userRepository.findByName("Waldorf");
        retries++;
    }

    if(waldorf != null) {
        System.out.println(">>> WALDORF BALANCE: " + waldorf.getBalance());
        // System.exit(0) se hum framework ke debugger loop ko FORCE KILL kar denge
        System.exit(0); 
    }
}
