package com.DataAlchemist.reader_service.data_reader.file_parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

public interface FileParser {
    Map<String, Class<?>> getSchema(InputStream inputStream) throws IOException;
    Stream<Map<String, Object>> parse(InputStream inputStream) throws IOException;
}
