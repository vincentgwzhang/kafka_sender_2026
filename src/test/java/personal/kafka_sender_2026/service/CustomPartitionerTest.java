package personal.kafka_sender_2026.service;

import org.apache.kafka.common.Cluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import personal.kafka_sender_2026.dto.OrderDto;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CustomPartitionerTest {

    private static final String TOPIC = "order-topic";
    private static final JsonMapper JSON_MAPPER = new JsonMapper();

    @Mock
    private Cluster cluster;

    private final CustomPartitioner partitioner = new CustomPartitioner();

    @BeforeEach
    void setUp() {
        when(cluster.partitionCountForTopic(TOPIC)).thenReturn(5);
    }

    @ParameterizedTest
    @CsvSource({
            "0, 0",
            "7, 2",
            "14, 4",
            "-3, 2"
    })
    void partition_mapsOrderIdMod5ToPartitionIndex(int orderId, int expectedPartition) throws Exception {
        int partition = partitioner.partition(TOPIC, null, null, null, payload(orderId), cluster);
        assertThat(partition).isEqualTo(expectedPartition);
    }

    @Test
    void partition_emptyPayloadUsesOrderIdZero() {
        int partition = partitioner.partition(TOPIC, null, null, null, new byte[0], cluster);
        assertThat(partition).isZero();
    }

    @Test
    void partition_throwsWhenTopicHasFewerThanFivePartitions() throws Exception {
        when(cluster.partitionCountForTopic(TOPIC)).thenReturn(4);

        assertThatThrownBy(() ->
                partitioner.partition(TOPIC, null, null, null, payload(14), cluster))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires at least 5 partitions");
    }

    private static byte[] payload(int orderId) throws Exception {
        OrderDto order = new OrderDto();
        order.setOrderId(orderId);
        return JSON_MAPPER.writeValueAsBytes(order);
    }
}
