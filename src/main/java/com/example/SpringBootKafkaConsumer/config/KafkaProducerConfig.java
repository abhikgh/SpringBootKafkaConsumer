package com.example.SpringBootKafkaConsumer.config;


import com.example.SpringBootKafkaConsumer.model.Toy;
import com.example.SpringBootKafkaConsumer.model.User;
import com.example.SpringBootKafkaConsumer.partition.KafkaCustomPartitioner;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaProducerConfig {

    @Value("${kafka.topic.send}")
    private String topicSend;

    @Bean
    public NewTopic newTopic(){
        return new NewTopic(topicSend, 3, (short) 1);
    }

	@Bean(name = "kafkaUserTemplate")
	public KafkaTemplate<String, User> kafkaUserTemplate(){
		Map<String, Object> map = new HashMap<>();
		map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaCustomPartitioner.class);
		ProducerFactory<String, User> producerFactory = new DefaultKafkaProducerFactory<>(map);
		return new KafkaTemplate<>(producerFactory);
	}

	@Bean(name = "kafkaToyTemplate")
	public KafkaTemplate<String, Toy> kafkaToyTemplate(){
		Map<String, Object> map = new HashMap<>();
		map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaCustomPartitioner.class);
		ProducerFactory<String, Toy> producerFactory = new DefaultKafkaProducerFactory<>(map);
		return new KafkaTemplate<>(producerFactory);
	}

	@Bean(name = "kafkaStringTemplate")
	public KafkaTemplate<String, String> kafkaStringTemplate(){
		Map<String, Object> map = new HashMap<>();
		map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		map.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, KafkaCustomPartitioner.class);
		ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(map);
		return new KafkaTemplate<>(producerFactory);
	}

	
}
