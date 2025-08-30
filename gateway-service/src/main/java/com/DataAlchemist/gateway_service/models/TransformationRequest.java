package com.DataAlchemist.gateway_service.models;

import com.DataAlchemist.gateway_service.models.enums.TransformationStatus;
import com.DataAlchemist.gateway_service.models.enums.TransformationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransformationRequest {

    private Long transformationRequestId;
    private TransformationType transformationType;
    private List<String> pipeline;
    private DataSource dataSource;
    private String schedulePattern;
    private TransformationStatus transformationStatus;
}
