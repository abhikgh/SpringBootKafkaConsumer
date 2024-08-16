package com.example.SpringBootKafkaConsumer.consumer;


import com.ingka.spe.model.icart.OrderInput;
import com.ingka.spe.model.icart.Toy;
import com.ingka.spe.model.icart.User;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
public class KafkaConsumerProducer {

    @Value("${kafka.topic.send}")
    private String topicSend;

    @Autowired
    @Qualifier("kafkaUserTemplate")
    private KafkaTemplate<String, User> kafkaUserTemplate;

    @Autowired
    @Qualifier("kafkaToyTemplate")
    private KafkaTemplate<String, Toy> kafkaToyTemplate;

    @Autowired
    @Qualifier("kafkaOrderTemplate")
    private KafkaTemplate<String, OrderInput> kafkaOrderTemplate;

    @Autowired
    @Qualifier("kafkaStringTemplate")
    private KafkaTemplate<String, String> kafkaStringTemplate;

    @SneakyThrows
    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.receive}",
                                             partitionOffsets = @PartitionOffset(partition = "0" ,initialOffset = "0")),
            containerFactory = "kafkaStringListenerContainerFactory", groupId = "group1")
    public void consumeString(@Payload String input){
        System.out.println("String " + input + " received successfully");
        input = input.toUpperCase();
        kafkaStringTemplate.send(topicSend, UUID.randomUUID().toString(), input);

    }
	@SneakyThrows
	@KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.receive}",
                                              partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "0")),
            containerFactory = "kafkaUserListenerContainerFactory", groupId = "group1")
	public void consumeUser(@Payload User user){
		System.out.println("User received successfully");
        user.setUserId(user.getUserId().toUpperCase());
		user.setUserName(user.getUserName().toUpperCase());
        user.setDesignation(user.getDesignation().toUpperCase());
        kafkaUserTemplate.send(topicSend, UUID.randomUUID().toString(), user);
	}

    @SneakyThrows
    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.receive}",
                                              partitionOffsets = @PartitionOffset(partition = "2" ,initialOffset = "0")),
            containerFactory = "kafkaToyListenerContainerFactory", groupId = "group1")
    public void consumeToy(@Payload Toy toy){
        System.out.println("Toy received successfully");
        toy.setToyId(toy.getToyId().toUpperCase());
        toy.setToyName(toy.getToyName().toUpperCase());
        toy.setToyType(toy.getToyType().toUpperCase());
        kafkaToyTemplate.send(topicSend, UUID.randomUUID().toString(), toy);
    }

    @SneakyThrows
    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.topic.receive}",
                    partitionOffsets = @PartitionOffset(partition = "3" ,initialOffset = "0")),
            containerFactory = "kafkaOrderListenerContainerFactory", groupId = "group1")
    public void consumeOrder(@Payload OrderInput orderInput){
            System.out.println("OrderInput received successfully");
        orderInput.setStatus(orderInput.getStatus().concat("-").concat("CONSUMED"));
        //int randomNum = rand.nextInt((max - min) + 1) + min;
        int randomNum = new Random().nextInt((9000 - 1000) + 1) + 1000;
        orderInput.setConsumerId(String.valueOf(randomNum));
        kafkaOrderTemplate.send(topicSend, UUID.randomUUID().toString(), orderInput);
    }



}
