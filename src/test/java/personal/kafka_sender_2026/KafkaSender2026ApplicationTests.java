package personal.kafka_sender_2026;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(partitions = 5, topics = "order-topic")
class KafkaSender2026ApplicationTests {

    @Test
    void contextLoads() {
    }

}
