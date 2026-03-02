package com.DataAlchemist.reader_service.services;

import com.DataAlchemist.reader_service.constants.AppConstants;
import com.DataAlchemist.reader_service.models.DataStreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class DataResponseProducerService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataResponseProducerService.class);
    private final KafkaTemplate<String, DataStreamResponse> kafkaTemplate;

    public DataResponseProducerService(KafkaTemplate<String, DataStreamResponse> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produceDataStreamResponse(DataStreamResponse dataStreamResponse) {
        var future = kafkaTemplate.send(AppConstants.DATA_STREAM_RESPONSE_TOPIC, dataStreamResponse);
        future.whenComplete((result, exception) -> {
            if(exception == null) {
                future.complete(result);
            } else {
                future.completeExceptionally(exception);
                LOGGER.error("streaming data failed at {}.", dataStreamResponse);
            }
        });
    }

}
