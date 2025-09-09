package com.DataAlchemist.transformation_service.models;

import com.DataAlchemist.transformation_service.models.enums.SourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DataSource {
    private SourceType sourceType;
    private String dataSource;
    private String authorizationToken;
}
