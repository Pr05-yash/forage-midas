package com.jpmc.midascore;

import com.jpmc.midascore.entity.User;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"})
public class TaskFourTests {

    @Autowired private KafkaProducer kafkaProducer;
    @Autowired private UserPopulator userPopulator;
    @Autowired private FileLoader fileLoader;
    @Autowired private UserRepository userRepository;

    @Test
    void task_four_verifier() throws InterruptedException {
        userPopulator.populate();
        
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        // Time badha kar 15 seconds kar diya hai taaki Kafka process ho jaye
        Thread.sleep(15000); 

        // Wilbur ka balance check
        User wilbur = userRepository.findByName("Wilbur");
        
        if (wilbur != null) {
            System.out.println("----------------------------------------------------------");
            System.out.println("FINAL BALANCE OF WILBUR: " + (int) wilbur.getBalance());
            System.out.println("----------------------------------------------------------");
        } else {
            // Agar Wilbur nahi mila, toh saare users print karke dekho
            List<User> allUsers = userRepository.findAll();
            System.out.println("Wilbur nahi mila. Available users: " + allUsers);
        }
    }
}
