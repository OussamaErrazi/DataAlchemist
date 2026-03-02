package com.DataAlchemist.transformation_service.models;

import com.DataAlchemist.transformation_service.models.enums.FileType;
import com.DataAlchemist.transformation_service.models.enums.StreamType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataStreamResponse {
    private String requestId;
    private FileType fileType;
    private long rowInder;
    private StreamType streamType;
    private Map<String, String> payload;
}
