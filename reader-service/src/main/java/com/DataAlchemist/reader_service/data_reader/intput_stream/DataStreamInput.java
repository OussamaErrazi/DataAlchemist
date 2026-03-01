package com.DataAlchemist.reader_service.data_reader.intput_stream;

import java.io.IOException;
import java.io.InputStream;

public interface DataStreamInput {
    InputStream getInputStream() throws IOException;
}
