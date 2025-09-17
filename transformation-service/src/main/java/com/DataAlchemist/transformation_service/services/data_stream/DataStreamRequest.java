package com.DataAlchemist.transformation_service.services.data_stream;

import com.DataAlchemist.transformation_service.models.DataSource;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataStreamRequest {
    private String requestId;
    private DataSource dataSource;

    public String getRequestId() {
        return requestId;
    }

    public DataSource getDataSource() {
        return dataSource;
    }
}
