package personal.kafka_sender_2026.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import personal.kafka_sender_2026.dto.OrderDto;
import personal.kafka_sender_2026.kafka.KafkaMessageHeaders;
import personal.kafka_sender_2026.kafka.OrderMessageConsumer;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
        partitions = 5,
        topics = KafkaServiceTest.ORDER_TOPIC
)
class KafkaServiceTest {

    static final String ORDER_TOPIC = "order-topic";

    @Autowired
    private KafkaService kafkaService;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private JsonMapper jsonMapper;

    @Test
    void sendMessages_writesToPartitionDerivedFromOrderId() throws Exception {
        assertMessageOnPartition(12, 2);
    }

    @Test
    void sendMessages_whenOrderIdMod5IsFour_usesPartitionFour() throws Exception {
        assertMessageOnPartition(19, 4);
    }

    private void assertMessageOnPartition(int orderId, int expectedPartition) throws Exception {
        try (Consumer<String, byte[]> consumer = createConsumer()) {
            embeddedKafka.consumeFromAnEmbeddedTopic(consumer, true, ORDER_TOPIC);
            UUID traceId = kafkaService.sendMessages(orderId);

            ConsumerRecord<String, byte[]> record =
                    KafkaTestUtils.getSingleRecord(consumer, ORDER_TOPIC, Duration.ofSeconds(10));

            assertThat(record.partition()).isEqualTo(expectedPartition);
            assertThat(OrderMessageConsumer.headerAsUtf8(record, KafkaMessageHeaders.TRACE_ID)).isEqualTo(traceId.toString());
            OrderDto received = jsonMapper.readValue(record.value(), OrderDto.class);
            assertThat(received.getOrderId()).isEqualTo(orderId);
        }
    }

    private Consumer<String, byte[]> createConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(embeddedKafka, "kafka-service-test", true);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return new DefaultKafkaConsumerFactory<String, byte[]>(props).createConsumer();
    }
}
