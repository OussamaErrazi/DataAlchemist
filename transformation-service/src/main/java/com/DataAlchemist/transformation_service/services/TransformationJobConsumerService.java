package com.DataAlchemist.transformation_service.services;


import com.DataAlchemist.transformation_service.services.data_stream.DataStreamRequest;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.DataAlchemist.transformation_service.models.TransformationRequest;

import java.util.UUID;
import java.util.function.Function;


@Component
public class TransformationJobConsumerService {

    private final Function<TransformationRequest, DataStreamRequest> streamRequestFunction = (tRequest) ->
            DataStreamRequest.builder()
                    .requestId("REQ-"+ UUID.randomUUID())
                    .dataSource(tRequest.getDataSource())
                    .build();
    private final Logger LOGGER = LoggerFactory.getLogger(TransformationJobConsumerService.class);

    private final DataStreamRequestProducerService dataStreamProducer;

    public TransformationJobConsumerService(DataStreamRequestProducerService dataStreamProducer) {
        this.dataStreamProducer = dataStreamProducer;
    }

    @PostConstruct
    public void init() {
        LOGGER.info("Transformation Consumer Service Initialized");
        LOGGER.info("Ready to consume from topic: transformation-job-topic");
    }

    @KafkaListener(topics = "transformation-job-topic", groupId = "transformation-workers")
    public void consume(TransformationRequest transformationRequest) {
        LOGGER.info("found new transformation job with id {}.", transformationRequest.getTransformationRequestId());
        LOGGER.info("starting to request a data reading stream.");

        DataStreamRequest dataStreamRequest = streamRequestFunction.apply(transformationRequest);

        dataStreamProducer.produceDataStreamRequest(dataStreamRequest);
    }


}
