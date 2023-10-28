package com.example.SpringBootKafkaConsumer.partition;


import com.example.SpringBootKafkaConsumer.model.Toy;
import com.example.SpringBootKafkaConsumer.model.User;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class KafkaCustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (value instanceof String) {
            return 0;
        } else if (value instanceof User) {
            return 1;
        } else if (value instanceof Toy) {
            return 2;
        } else {
            return 0;
        }

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void close() {

    }
}
