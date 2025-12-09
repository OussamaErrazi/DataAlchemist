package com.DataAlchemist.reader_service.models;


import com.DataAlchemist.reader_service.models.enums.SourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataSource {
    private SourceType sourceType;
    private String dataSource;
    private String authorizationToken;
}
