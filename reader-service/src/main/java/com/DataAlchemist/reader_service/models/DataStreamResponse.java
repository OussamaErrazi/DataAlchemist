package com.DataAlchemist.reader_service.models;

import com.DataAlchemist.reader_service.models.enums.FileType;
import com.DataAlchemist.reader_service.models.enums.StreamType;
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
    private long rowIndex;
    private StreamType streamType;
    private Map<String, String> payload;
}
