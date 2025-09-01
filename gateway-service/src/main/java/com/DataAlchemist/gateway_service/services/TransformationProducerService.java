package com.DataAlchemist.gateway_service.services;


import com.DataAlchemist.gateway_service.models.TransformationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class TransformationProducerService {

    private final KafkaTemplate<String, TransformationRequest> kafkaTemplate;
    private final Logger LOGGER = LoggerFactory.getLogger(TransformationProducerService.class);

    public TransformationProducerService(KafkaTemplate<String, TransformationRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produceMessage(String topicName, TransformationRequest transformationRequest) {
        var future = kafkaTemplate.send(topicName, transformationRequest);
        future.whenComplete((sendResult, exception) -> {
            if (exception == null) {
                future.complete(sendResult);
                LOGGER.info("Transformation request with id {} was sent to topic: {} successfully.", transformationRequest.getTransformationRequestId() ,topicName);
            } else {
                future.completeExceptionally(exception);
                LOGGER.error("Transformation request failed. {}", exception.getMessage());
            }
        });
    };
}
