package personal.kafka_sender_2026.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.instancio.Instancio;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import personal.kafka_sender_2026.dto.OrderDto;
import personal.kafka_sender_2026.kafka.KafkaMessageHeaders;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 * 知识点：
 * 1, 对于 public CompletableFuture<SendResult<K, V>> send(String topic, K key, @Nullable V data)
 *          同样的 key参数下 永远进入同一个 partition
 *          注意： 如果一旦有 personal.kafka_sender_2026.service.CustomPartitioner 参数，那么进入哪个partition 将会调用 CustomPartitioner
 *    对于 public CompletableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, @Nullable V data)
 *          此时 partition 参数的优先级更高，而key 参数不再参与进入那一个 partition 的计算, 但 key 写入 message metadata
 *          注意： partition 参数是最高级的，无论有没有使用 personal.kafka_sender_2026.service.CustomPartitioner
 *    对于 public CompletableFuture<SendResult<K, V>> send(String topic, @Nullable V data)
 *          此时使用 Sticky Partitioner,首先随意制定一个Partition, 然后一直往那个 Partion 塞 message
 *          注意： 如果此时有 personal.kafka_sender_2026.service.CustomPartitioner, 那么将会调用 CustomPartitioner
 *
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaService {

    private static final String TOPIC = "order-topic";
    private final JsonMapper jsonMapper;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public UUID sendMessages(int num) throws Exception {
        OrderDto orderDto = Instancio.of(OrderDto.class).create();
        orderDto.setOrderId(num);
        byte[] payload = jsonMapper.writeValueAsBytes(orderDto);

        UUID traceId = UUID.randomUUID();
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(TOPIC, payload);
        record.headers().add(
                KafkaMessageHeaders.TRACE_ID,
                traceId.toString().getBytes(StandardCharsets.UTF_8));

        CompletableFuture<SendResult<String, byte[]>> result = kafkaTemplate.send(record);

        // 如果正常回 SendResult 就证明发送成功了。因为不成功会抛出 Exception
        SendResult<String, byte[]> data = result.get(3L, TimeUnit.SECONDS);
        log.info(
                "Sent traceId={}, orderId={}, partition={}, offset={}",
                traceId,
                orderDto.getOrderId(),
                data.getRecordMetadata().partition(),
                data.getRecordMetadata().offset());
        return traceId;
    }

}
