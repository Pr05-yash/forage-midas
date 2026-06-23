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
    void task_three_verifier() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // 5 seconds ka wait
        Thread.sleep(5000); 

        var waldorf = userRepository.findByName("Waldorf");
        
        // Yahan assert use karein, isse framework ko signal milega ki test complete ho gaya hai
        assertNotNull(waldorf, "Waldorf database mein nahi mila!");
        System.out.println(">>> WALDORF FINAL BALANCE IS: " + waldorf.getBalance());
    }
}
