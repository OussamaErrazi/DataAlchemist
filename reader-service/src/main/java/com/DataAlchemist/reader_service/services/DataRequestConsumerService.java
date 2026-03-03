package com.DataAlchemist.reader_service.services;

import com.DataAlchemist.reader_service.constants.AppConstants;
import com.DataAlchemist.reader_service.data_reader.FileReader;
import com.DataAlchemist.reader_service.models.DataStreamRequest;
import com.DataAlchemist.reader_service.models.DataStreamResponse;
import com.DataAlchemist.reader_service.models.enums.StreamType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class DataRequestConsumerService {

    private final Logger LOGGER = LoggerFactory.getLogger(DataRequestConsumerService.class);

    private final DataResponseProducerService dataResponseProducerService;

    public DataRequestConsumerService(DataResponseProducerService dataResponseProducerService) {
        this.dataResponseProducerService = dataResponseProducerService;
    }


    @KafkaListener(topics = AppConstants.DATA_STREAM_REQUEST_TOPIC, groupId = AppConstants.DATA_RETRIEVING_WORKERS_GROUP)
    public void consume(DataStreamRequest dataStreamRequest) {
        LOGGER.info("request received for reading and streaming this data file: {}.", dataStreamRequest);
        FileReader fileReader;
        try {
            fileReader = FileReader.of(dataStreamRequest.getDataSource());
//            LOGGER.info("here is the dataset schema: {}", fileReader.getSchema());
            Map<String, String> schemaPayload = new LinkedHashMap<>();
            for(Map.Entry<String, Class<?>> entry : fileReader.getSchema().entrySet()) {
                schemaPayload.put(entry.getKey(), entry.getValue().getSimpleName());
            }
            dataResponseProducerService.produceDataStreamResponse(
                    DataStreamResponse.builder()
                            .requestId(dataStreamRequest.getRequestId())
                            .fileType(fileReader.getFileType())
                            .rowIndex(0)
                            .payload(schemaPayload)
                            .streamType(StreamType.SCHEMA)
                            .build()
            );
//            LOGGER.info("dataset rows: ");
            AtomicLong index = new AtomicLong(1);
            try(Stream<Map<String, Object>> stream = fileReader.streamEntries()) {
                stream.forEach(e -> {
                    LOGGER.info("entry before to payload {}",e.toString());
                    Map<String, String> rowPayload = new LinkedHashMap<>();
                    for(Map.Entry<String, Object> entry : e.entrySet()){
                        rowPayload.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().toString());
                    }
                    LOGGER.info("payload to send {}", rowPayload);
                    dataResponseProducerService.produceDataStreamResponse(
                            DataStreamResponse.builder()
                                    .requestId(dataStreamRequest.getRequestId())
                                    .streamType(StreamType.ROW)
                                    .rowIndex(index.get())
                                    .payload(rowPayload)
                                    .fileType(fileReader.getFileType())
                                    .build()
                    );
                    LOGGER.info("row number {} was sent successfully.", index.get());
                    index.getAndIncrement();
                });
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
            }

            dataResponseProducerService.produceDataStreamResponse(
                    DataStreamResponse.builder()
                            .requestId(dataStreamRequest.getRequestId())
                            .streamType(StreamType.COMPLETED)
                            .rowIndex(index.get())
                            .fileType(fileReader.getFileType())
                            .build()
            );
        } catch (IOException e) {
            LOGGER.error("error parsing file {}", dataStreamRequest.getDataSource().getDataSource());
            LOGGER.error(e.getMessage());
            return;
        }
        LOGGER.info("the file data streaming ended.");
    }
}
