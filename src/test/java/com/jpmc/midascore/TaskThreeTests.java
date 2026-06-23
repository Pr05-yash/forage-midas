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
    void task_three_verifier() {
        System.out.println(">>> [DEBUG] TEST METHOD START HO GAYA HAI");
        // Filhal sirf yahi rakhein
    }
}
