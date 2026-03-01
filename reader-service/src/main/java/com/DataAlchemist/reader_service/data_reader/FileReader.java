package com.DataAlchemist.reader_service.data_reader;

import com.DataAlchemist.reader_service.data_reader.file_parser.CsvParser;
import com.DataAlchemist.reader_service.data_reader.file_parser.FileParser;
import com.DataAlchemist.reader_service.data_reader.file_parser.JsonParser;
import com.DataAlchemist.reader_service.data_reader.intput_stream.DataStreamInput;
import com.DataAlchemist.reader_service.data_reader.intput_stream.LocalStreamInput;
import com.DataAlchemist.reader_service.data_reader.intput_stream.RemoteStreamInput;
import com.DataAlchemist.reader_service.models.DataSource;
import com.DataAlchemist.reader_service.models.enums.SourceType;
import lombok.Getter;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Stream;

public class FileReader {

    @Getter
    private final Map<String, Class<?>> schema;
    private final DataStreamInput input;
    private final FileParser parser;

    public FileReader(DataStreamInput input, FileParser parser) throws IOException {
        this.input = input;
        this.parser = parser;
        this.schema = parser.getSchema(input.getInputStream());
    }

    public FileReader(Map<String, Class<?>> schema, DataStreamInput input, FileParser parser) {
        this.schema = schema;
        this.input = input;
        this.parser = parser;
    }

    public Stream<Map<String, Object>> streamEntries() throws IOException {
        return parser.parse(input.getInputStream());
    }

    public static FileReader of(DataSource dataSource) throws IOException {
        return new FileReader(
                dataSource.getSourceType() == SourceType.Local ? new LocalStreamInput(dataSource.getDataSource()) : new RemoteStreamInput(dataSource.getDataSource(), dataSource.getAuthorizationToken()),
                getFileParser(dataSource.getDataSource(), dataSource.getSourceType())
                );
    }

    private static FileParser getFileParser(String dataPath, SourceType type) throws IOException {
        if (dataPath == null || dataPath.isEmpty()) throw new RuntimeException("please provide a file path in the request body field [dataURL].");
        if (type == SourceType.Local) return dataPath.toLowerCase().endsWith(".json") ? new JsonParser() : new CsvParser();
        else {
            return detectRemoteFileType(dataPath);
        }
    }

    private static FileParser detectRemoteFileType(String url) throws IOException {
        URLConnection connection = new URL(url).openConnection();

        String contentType = connection.getContentType();
        if (contentType != null) {
            if (contentType.contains("json")) {
                return new JsonParser();
            }
            if (contentType.contains("csv")) {
                return new CsvParser();
            }
        }

        try (InputStream inputStream = connection.getInputStream()) {
            return detectFromContent(inputStream);
        }
    }

    public static FileParser detectFromContent(InputStream inputStream) throws IOException {
        BufferedInputStream bufferedStream = new BufferedInputStream(inputStream);
        bufferedStream.mark(2048);

        try {
            byte[] buffer = new byte[1024];
            int bytesRead = bufferedStream.read(buffer);
            String content = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8).trim();

            bufferedStream.reset();

            if (content.startsWith("[") || content.startsWith("{")) return new JsonParser();

            if (isLikelyCsv(content)) return new CsvParser();

            throw new IllegalArgumentException("Unknown file format");

        } catch (Exception e) {
            bufferedStream.reset();
            throw e;
        }
    }

    private static boolean isLikelyCsv(String content) {
        String[] lines = content.split("\n", 3);
        if (lines.length < 1) return false;

        String firstLine = lines[0].trim();

        boolean hasCommas = firstLine.contains(",");
//        boolean hasQuotes = firstLine.contains("\"");
        boolean noJsonChars = !firstLine.contains("{") && !firstLine.contains("[");

        return hasCommas && noJsonChars;
    }
}
