package personal.kafka_sender_2026.service;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import personal.kafka_sender_2026.dto.OrderDto;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/**
 * Routes by orderId in the JSON payload (KafkaService sends value only, no key):
 * N = orderId % 5 → the (N+1)th partition, i.e. Kafka partition index N (0–4).
 * Requires the topic to have at least {@value #BUCKET_MODULO} partitions.
 */
public class CustomPartitioner implements Partitioner {

    private static final int BUCKET_MODULO = 5;
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int orderId = parseOrderId(valueBytes);
        int n = Math.floorMod(orderId, BUCKET_MODULO);
        int partitionCount = cluster.partitionCountForTopic(topic);
        if (n >= partitionCount) {
            throw new IllegalStateException(
                    "orderId=%d => N=%d requires at least %d partitions on topic '%s', but found %d"
                            .formatted(orderId, n, BUCKET_MODULO, topic, partitionCount));
        }
        return n;
    }

    private static int parseOrderId(byte[] valueBytes) {
        if (valueBytes == null || valueBytes.length == 0) {
            return 0;
        }
        try {
            OrderDto dto = JSON_MAPPER.readValue(valueBytes, OrderDto.class);
            return dto.getOrderId();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read orderId from message payload", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
