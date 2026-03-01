package com.DataAlchemist.reader_service.data_reader.intput_stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class LocalStreamInput implements DataStreamInput{
    private final Path path;

    public LocalStreamInput(String dataInput) {
        this.path = Path.of(dataInput);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return Files.newInputStream(path);
    }
}
