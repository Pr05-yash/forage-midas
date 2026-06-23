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
    
    // Kafka process hone ke liye 5 second ka wait
    Thread.sleep(5000); 

    var waldorf = userRepository.findByName("Waldorf");
    
    // Yahan hum result print kar rahe hain
    if (waldorf != null) {
        System.out.println(">>> WALDORF FINAL BALANCE: " + (int) Math.floor(waldorf.getBalance()));
    }
    
    // ZAROORI: Test ko force finish karne ke liye hum yahan 'return' kar sakte hain 
    // ya phir simple log ke baad thread sleep hata sakte hain.
    System.out.println("TEST KAHATAM HUA.");
}
