package personal.kafka_sender_2026.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.instancio.Instancio;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import personal.kafka_sender_2026.dto.OrderDto;
import tools.jackson.databind.json.JsonMapper;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaService {

    private static final String TOPIC = "order-topic";
    private final JsonMapper jsonMapper;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public void sendMessages(int num) throws Exception {
        OrderDto orderDto = Instancio.of(OrderDto.class).create();
        byte[] payload = jsonMapper.writeValueAsBytes(orderDto);
        CompletableFuture<SendResult<String, byte[]>> result = kafkaTemplate.send(TOPIC, payload);
        SendResult<String, byte[]> data = result.get(3L, TimeUnit.SECONDS);
        log.info("Sent message: \\d{} on partition \\d{}, with offset {}\\d and content = {}", orderDto, data.getRecordMetadata().partition(), data.getRecordMetadata().offset(), orderDto);
    }

}
