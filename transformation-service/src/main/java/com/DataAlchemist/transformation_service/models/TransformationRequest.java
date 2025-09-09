package com.DataAlchemist.transformation_service.models;

import com.DataAlchemist.transformation_service.models.enums.TransformationStatus;
import com.DataAlchemist.transformation_service.models.enums.TransformationType;
import lombok.*;

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

    public Long getTransformationRequestId() {
        return transformationRequestId;
    }

    public TransformationType getTransformationType() {
        return transformationType;
    }

    public List<String> getPipeline() {
        return pipeline;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public String getSchedulePattern() {
        return schedulePattern;
    }

    public TransformationStatus getTransformationStatus() {
        return transformationStatus;
    }
}
