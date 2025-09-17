package com.DataAlchemist.transformation_service.services;

import com.DataAlchemist.transformation_service.constants.AppConstants;
import com.DataAlchemist.transformation_service.services.data_stream.DataStreamRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DataStreamRequestProducerService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataStreamRequestProducerService.class);
    private final KafkaTemplate<String, DataStreamRequest> kafkaTemplate;


    public DataStreamRequestProducerService(KafkaTemplate<String, DataStreamRequest> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produceDataStreamRequest(DataStreamRequest dataStreamRequest) {
        var future = kafkaTemplate.send(AppConstants.DATA_STREAM_REQUEST_TOPIC, dataStreamRequest);
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                future.complete(result);
                LOGGER.info("successfully sent request for data: {}", dataStreamRequest.getDataSource());
            } else {
                future.completeExceptionally(exception);
                LOGGER.error("request the following data stream failed: {}.", dataStreamRequest.getDataSource());
            }
        });
    }
}
