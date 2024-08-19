package com.example.SpringBootKafkaConsumer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ingka.spe.model.icart.OrderInput;
import com.ingka.spe.model.icart.OrderOutput;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.Span;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.Random;
import java.util.UUID;

@RestController
@RequestMapping("/kafka")
public class NavController {

    @Autowired
    private JaegerTracer jaegerTracer;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RestTemplate restTemplate;

    @PostMapping(value = "/updateOrderClient", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public OrderOutput updateOrder(@RequestBody OrderInput orderInput) throws JsonProcessingException {
        Span span = jaegerTracer.buildSpan("updateOrderClient").start();
        orderInput.setStatus(orderInput.getStatus().concat("-").concat("CONSUMED"));
        //int randomNum = rand.nextInt((max - min) + 1) + min;
        int randomNum = new Random().nextInt((9000 - 1000) + 1) + 1000;
        orderInput.setConsumerId(String.valueOf(randomNum));
        orderInput.setOrderDate("2023-02-12");
        orderInput.setOrderStatus(100);

        //call the updateOrder service
        String endPoint = "http://localhost:9071/kafka/updateOrder";

        URI uri = UriComponentsBuilder
                .fromUriString(endPoint)
                .build()
                .encode()
                .toUri();


        String requestJson = objectMapper.writeValueAsString(orderInput);

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<String> httpEntity = new HttpEntity<>(requestJson, httpHeaders);

        Span spanConsumeOrder2 = jaegerTracer.buildSpan("consumeOrder2").asChildOf(span).start();
        OrderOutput orderOutput = restTemplate.exchange(uri, HttpMethod.POST, httpEntity, OrderOutput.class).getBody();
        spanConsumeOrder2.finish();

        System.out.println("----------OrderOutput details------------");
        System.out.println(orderOutput.getOrderId()+"-"+orderOutput.isOrderStatus()+"-"+orderOutput.getConsumerId()+"-"+orderOutput.getOrderDate()+"-"+orderOutput.getStatus());

        span.finish();
        return orderOutput;
    }
}
