package com.DataAlchemist.reader_service.config;


import com.DataAlchemist.reader_service.constants.AppConstants;
import com.DataAlchemist.reader_service.models.DataStreamRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value(value = "${kafka.server.url:localhost:9092}")
    private String KAFKA_SERVER_URL;

    @Bean
    public ConsumerFactory<String, DataStreamRequest> consumerFactory() {
        Map<String, Object> consumerConfigMap = new HashMap<>();
        consumerConfigMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        consumerConfigMap.put(ConsumerConfig.GROUP_ID_CONFIG, AppConstants.DATA_RETRIEVING_WORKERS_GROUP);

        consumerConfigMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        consumerConfigMap.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        consumerConfigMap.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        consumerConfigMap.put(JsonDeserializer.TRUSTED_PACKAGES, "com.DataAlchemist.*");
        consumerConfigMap.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.DataAlchemist.reader_service.models.DataStreamRequest");

        return new DefaultKafkaConsumerFactory<>(consumerConfigMap);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, DataStreamRequest>> kafkaListenerContainerFactory(
            ConsumerFactory<String, DataStreamRequest> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, DataStreamRequest> containerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        containerFactory.setConsumerFactory(consumerFactory);
        containerFactory.setConcurrency(3);
        return containerFactory;
    }
}
