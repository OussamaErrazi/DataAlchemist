package com.DataAlchemist.gateway_service.controllers;


import com.DataAlchemist.gateway_service.constants.AppConstants;
import com.DataAlchemist.gateway_service.controllers.requests.TransformationCreationRequest;
import com.DataAlchemist.gateway_service.controllers.requests.TransformationScheduleRequest;
import com.DataAlchemist.gateway_service.models.DataSource;
import com.DataAlchemist.gateway_service.models.TransformationRequest;
import com.DataAlchemist.gateway_service.models.enums.SourceType;
import com.DataAlchemist.gateway_service.models.enums.TransformationStatus;
import com.DataAlchemist.gateway_service.models.enums.TransformationType;
import com.DataAlchemist.gateway_service.services.TransformationProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("transformation/job")
public class TransformationRequestController {

    private final Logger LOGGER = LoggerFactory.getLogger(TransformationRequestController.class);
    private final TransformationProducerService transformationProducerService;

    public TransformationRequestController(TransformationProducerService transformationProducerService) {
        this.transformationProducerService = transformationProducerService;
    }

    @PostMapping("create")
    public ResponseEntity<String> createTransformationJob(@RequestBody TransformationCreationRequest transformationCreationRequest) {
        LOGGER.info("received a data transformation job: {}", transformationCreationRequest);
        transformationProducerService.produceMessage(
                AppConstants.TRANSFORMATION_JOB_TOPIC,
                getTransformationRequest(transformationCreationRequest)
        );
        return ResponseEntity.ok("transformation creation received.");
    }

    @PostMapping("schedule")
    public ResponseEntity<String> scheduleTransformationJob(@RequestBody TransformationScheduleRequest transformationScheduleRequest) {
        LOGGER.info("received a data transformation schedule: {}", transformationScheduleRequest);
        transformationProducerService.produceMessage(
                AppConstants.TRANSFORMATION_SCHEDULE_TOPIC,
                getTransformationRequest(transformationScheduleRequest)
        );
        return ResponseEntity.ok("transformation schedule received.");
    }

    private TransformationRequest getTransformationRequest(TransformationCreationRequest transformationCreationRequest) {
        return TransformationRequest.builder()
                .transformationStatus(TransformationStatus.Pending)
                .transformationType(TransformationType.RealTime)
                .transformationRequestId(transformationCreationRequest.getTransformationId())
                .dataSource(DataSource.builder()
                        .authorizationToken(transformationCreationRequest.getAuthToken())
                        .dataSource(transformationCreationRequest.getDataURL())
                        .sourceType(transformationCreationRequest.isLocal() ? SourceType.Local : SourceType.Remote)
                        .build())
                .pipeline(transformationCreationRequest.getPipeline())
                .build();
    }

    private TransformationRequest getTransformationRequest(TransformationScheduleRequest transformationScheduleRequest) {
        return TransformationRequest.builder()
                .transformationStatus(TransformationStatus.Pending)
                .transformationType(TransformationType.Scheduled)
                .schedulePattern(transformationScheduleRequest.getSchedulePattern())
                .transformationRequestId(transformationScheduleRequest.getTransformationId())
                .dataSource(DataSource.builder()
                        .authorizationToken(transformationScheduleRequest.getAuthToken())
                        .dataSource(transformationScheduleRequest.getDataURL())
                        .sourceType(transformationScheduleRequest.isLocal() ? SourceType.Local : SourceType.Remote)
                        .build())
                .pipeline(transformationScheduleRequest.getPipeline())
                .build();
    }
}
