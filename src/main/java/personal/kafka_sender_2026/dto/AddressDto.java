package personal.kafka_sender_2026.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AddressDto {

    private String street;

    private String city;

    private String state;

    private String zipCode;

    private String country;

}
