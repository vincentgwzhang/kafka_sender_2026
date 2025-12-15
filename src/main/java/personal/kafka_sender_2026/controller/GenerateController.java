package personal.kafka_sender_2026.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import personal.kafka_sender_2026.service.KafkaService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/send")
public class GenerateController {

    private final KafkaService kafkaService;

    @PostMapping
    public void send(@RequestParam int num) throws Exception {
        kafkaService.sendMessages(num);
    }

}
