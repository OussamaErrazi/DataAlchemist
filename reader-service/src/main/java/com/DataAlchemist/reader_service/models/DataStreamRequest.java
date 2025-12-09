package com.DataAlchemist.reader_service.models;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataStreamRequest {
    private String requestId;
    private DataSource dataSource;
}
