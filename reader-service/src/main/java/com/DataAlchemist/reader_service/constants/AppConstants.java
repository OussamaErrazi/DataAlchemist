package com.DataAlchemist.reader_service.constants;

import java.util.List;

public interface AppConstants {
    int SAMPLE_LINES = 10;
    String DATA_STREAM_RESPONSE_TOPIC = "data-stream-response-topic";
    String DATA_STREAM_REQUEST_TOPIC = "data-stream-request-topic";
    String DATA_RETRIEVING_WORKERS_GROUP = "data-retrieving-workers";
    String CSV_LINE_SPLITTER = ",";

    List<String> DATE_FORMATS = List.of("yyyy-MM-dd", "MM/dd/yyyy", "dd-MM-yyyy");
}
