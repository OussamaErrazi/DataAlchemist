package com.DataAlchemist.transformation_service.config;


import com.DataAlchemist.transformation_service.constants.AppConstants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaConfig {

    @Bean
    public KafkaAdmin.NewTopics dataReadRequestTopic() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(AppConstants.DATA_STREAM_REQUEST_TOPIC).build(),
                TopicBuilder.name(AppConstants.DATA_STREAM_RESPONSE_TOPIC).build());
    }
}
