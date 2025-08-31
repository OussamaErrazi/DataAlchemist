package com.DataAlchemist.gateway_service.config;


import com.DataAlchemist.gateway_service.models.TransformationRequest;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${kafka.server.url:localhost:9092}")
    private String KAFKA_SERVER_URL;

    @Bean
    public ProducerFactory<String, TransformationRequest> producerFactory() {
        Map<String, Object> producerConfigMap = new HashMap<>();
        producerConfigMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        producerConfigMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerConfigMap.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(producerConfigMap);
    }

    @Bean
    public KafkaTemplate<String, TransformationRequest> kafkaTemplate(ProducerFactory<String, TransformationRequest> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
