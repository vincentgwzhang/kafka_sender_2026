package personal.kafka_sender_2026.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, byte[]> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "personal.kafka_sender_2026.service.CustomPartitioner");

        /**
         * 发送确认机制, 必须是字符串，必须是 all (或 -1) / 1 / 0
         * 0 代表无需确认
         * 1 代表主 leader 签收
         * all 或 -1 代表全员确认，包括主 leader 和 replication, 虽然安全但是最耗时
         */
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        /**
         * 保证不会处理重复信息
         * 注意：是不会处理重复信息， 但不是不能retry
         */
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        /**
         * 如果出错，重发信息
         */
        props.put(ProducerConfig.RETRIES_CONFIG, 5);

        /**
         * 这里是说 Batch size 达到多少的时候就发送信息，如果需要 size = 16KB,
         * 那么就是 props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16 * 1024);
         */
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);

        /**
         * producer 最多愿意等待多久来凑 batch，时间一到，无论 batch size 是否够都会发送
         * 如果是 5ms, 那么就是 props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
         */
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, byte[]> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
