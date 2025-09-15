package com.DataAlchemist.transformation_service.config;

import com.DataAlchemist.transformation_service.services.data_stream.DataStreamRequest;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${kafka.server.url:localhost:9092}")
    private String KAFKA_SERVER_URL;

    @Bean
    public ProducerFactory<String, DataStreamRequest> producerFactory() {
        Map<String, Object> producerConfigMap = new HashMap<>();
        producerConfigMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        producerConfigMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerConfigMap.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(producerConfigMap);
    }

    @Bean
    public KafkaTemplate<String, DataStreamRequest> kafkaTemplate(ProducerFactory<String, DataStreamRequest> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
