package com.DataAlchemist.reader_service.services;

import com.DataAlchemist.reader_service.constants.AppConstants;
import com.DataAlchemist.reader_service.data_reader.FileReader;
import com.DataAlchemist.reader_service.models.DataSource;
import com.DataAlchemist.reader_service.models.DataStreamRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

@Component
public class DataRequestConsumerService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataRequestConsumerService.class);


    @KafkaListener(topics = AppConstants.DATA_STREAM_REQUEST_TOPIC, groupId = AppConstants.DATA_RETRIEVING_WORKERS_GROUP)
    public void consume(DataStreamRequest dataStreamRequest) {
        LOGGER.info("request received for reading and streaming this data file: {}.", dataStreamRequest);
        FileReader fileReader;
        try {
            fileReader = FileReader.of(dataStreamRequest.getDataSource());
            LOGGER.info("here is the dataset schema: {}", fileReader.getSchema());
            LOGGER.info("dataset rows: ");
            try(Stream<Map<String, Object>> stream = fileReader.streamEntries()) {
                stream.forEach(e -> LOGGER.info(e.toString()));
            }
        } catch (IOException e) {
            LOGGER.error("error parsing file {}", dataStreamRequest.getDataSource().getDataSource());
            LOGGER.error(e.getMessage());
            return;
        }
        LOGGER.info("the file data streaming ended.");
    }
}
