package personal.kafka_sender_2026.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import personal.kafka_sender_2026.dto.OrderDto;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示如何从 Record Headers 读取发送端写入的 trace-id，并统计同一条消息被本进程消费的次数。
 * <p>
 * 注意：若 consumer group 重平衡、或 at-least-once 导致重复投递，receiveCount 可能 &gt; 1。
 */
@Component
@Profile("!test")
@Slf4j
public class OrderMessageConsumer {

    private final JsonMapper jsonMapper;
    private final Map<String, AtomicInteger> receiveCounts = new ConcurrentHashMap<>();

    public OrderMessageConsumer(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @KafkaListener(topics = "order-topic", groupId = "order-debug-group")
    public void onOrder(ConsumerRecord<String, byte[]> record) {
        String traceId = headerAsUtf8(record, KafkaMessageHeaders.TRACE_ID);
        int receiveCount = receiveCounts
                .computeIfAbsent(traceId, ignored -> new AtomicInteger())
                .incrementAndGet();

        OrderDto order = jsonMapper.readValue(record.value(), OrderDto.class);

        log.info(
                "Consumed traceId={}, receiveCount={}, topic={}, partition={}, offset={}, orderId={}",
                traceId,
                receiveCount,
                record.topic(),
                record.partition(),
                record.offset(),
                order.getOrderId());
    }

    /** 调试用：查看某个 traceId 在本 JVM 内被 @KafkaListener 调用了几次。 */
    public int receiveCountFor(String traceId) {
        AtomicInteger counter = receiveCounts.get(traceId);
        return counter == null ? 0 : counter.get();
    }

    public static String headerAsUtf8(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return "<missing>";
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
