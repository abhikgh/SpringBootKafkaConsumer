package com.example.SpringBootKafkaConsumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import org.springframework.ws.config.annotation.EnableWs;

import java.time.Duration;

@Configuration
@EnableWs
public class RestConfig {

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder){
        return restTemplateBuilder
                .setConnectTimeout(Duration.ofMillis(1000))
                .setReadTimeout(Duration.ofMillis(1000))
                .build();
    }

    @Bean
    public ObjectMapper objectMapper(){
        return JsonMapper
                .builder()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .build();
    }
}
