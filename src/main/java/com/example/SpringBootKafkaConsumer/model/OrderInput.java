package com.example.SpringBootKafkaConsumer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"orderId","status","consumerId"})
@ToString
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderInput {

    @JsonProperty("orderId")
    private String orderId;
    @JsonProperty("status")
    private String status;
    @JsonProperty("consumerId")
    private String consumerId;
}
