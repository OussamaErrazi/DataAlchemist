package com.DataAlchemist.transformation_service.services;


import com.DataAlchemist.transformation_service.constants.AppConstants;
import com.DataAlchemist.transformation_service.context.builder.PipelineBuilder;
import com.DataAlchemist.transformation_service.models.DataStreamResponse;
import com.DataAlchemist.transformation_service.context.PipelineContext;
import com.DataAlchemist.transformation_service.context.SchemaContext;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.StreamType;
import com.DataAlchemist.transformation_service.services.data_stream.DataStreamRequest;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.DataAlchemist.transformation_service.models.TransformationRequest;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Map<String, PipelineContext> piplinesMap = new ConcurrentHashMap<>();

    private final Map<String, SchemaContext> schemasMap = new ConcurrentHashMap<>();

    public TransformationJobConsumerService(DataStreamRequestProducerService dataStreamProducer) {
        this.dataStreamProducer = dataStreamProducer;
    }

    @PostConstruct
    public void init() {
        LOGGER.info("Transformation Consumer Service Initialized");
        LOGGER.info("Ready to consume from topic: transformation-job-topic");
    }

    @KafkaListener(topics = AppConstants.TRANSFORMATION_JOB_TOPIC, groupId = "transformation-workers", containerFactory = "kafkaListenerContainerFactory1")
    public void consume(TransformationRequest transformationRequest) {
        LOGGER.info("found new transformation job with id {}.", transformationRequest.getTransformationRequestId());
        LOGGER.info("starting to request a data reading stream.");

        DataStreamRequest dataStreamRequest = streamRequestFunction.apply(transformationRequest);
        //TODO save here the pipeline context
        PipelineContext pipelineCtx = PipelineBuilder.build(transformationRequest.getPipeline());
        piplinesMap.put(dataStreamRequest.getRequestId(), pipelineCtx);

        dataStreamProducer.produceDataStreamRequest(dataStreamRequest);
    }

    @KafkaListener(topics = AppConstants.DATA_STREAM_RESPONSE_TOPIC, groupId = "transformation-workers", containerFactory = "kafkaListenerContainerFactory2", concurrency = "1")
    public void consume(DataStreamResponse dataStreamResponse) {
        switch (dataStreamResponse.getStreamType()) {
            case StreamType.SCHEMA -> {
                SchemaContext schemaCxt = new SchemaContext();
                for(Map.Entry<String, String> entry : dataStreamResponse.getPayload().entrySet()){
                    schemaCxt.addColumn(entry.getKey(), entry.getValue());
                }
                schemasMap.put(dataStreamResponse.getRequestId(), schemaCxt);
            }
            case StreamType.ROW -> {
                //TODO deal with data transformation by first retrieving the pipeline context and schema context
                PipelineContext pipelineCxt = piplinesMap.get(dataStreamResponse.getRequestId());
                SchemaContext schemaCxt = schemasMap.get(dataStreamResponse.getRequestId());
                Row row = new Row();
                row.construct(schemaCxt, dataStreamResponse.getPayload());
                row = pipelineCxt.process(row);
                System.out.println(row);
            }
            case StreamType.COMPLETED -> {
                LOGGER.info("completed {}", dataStreamResponse.getPayload());
                //TODO delete schema context and pipeline context and signal the data completion to the data saving service
            }
        }
    }

}
