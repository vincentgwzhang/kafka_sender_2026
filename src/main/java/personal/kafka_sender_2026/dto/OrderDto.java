package personal.kafka_sender_2026.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {

    private UUID orderId;

    private Map<UUID, Integer> products;

    private List<ProductDto> productsList;

    private String customerName;

    private String couponCode;

    private AddressDto addressDto;

    private LocalDateTime orderTime;

}
