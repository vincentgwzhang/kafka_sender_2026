package personal.kafka_sender_2026.service;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import personal.kafka_sender_2026.dto.OrderDto;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int orderId = parseOrderId(valueBytes);
        int partitionCount = cluster.partitionCountForTopic(topic);
        return Math.floorMod(orderId, partitionCount);
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
