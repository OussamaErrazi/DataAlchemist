package com.DataAlchemist.reader_service.data_reader.intput_stream;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class RemoteStreamInput implements DataStreamInput {

    private final URL url;
    private final String authToken;

    public RemoteStreamInput(String dataUrl, String authToken) throws MalformedURLException {
        this.url = URI.create(dataUrl).toURL();
        this.authToken = authToken;
    }


    @Override
    public InputStream getInputStream() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        if (authToken != null && !authToken.isEmpty()) {
            connection.setRequestProperty("Authorization", authToken);
        }
        return connection.getInputStream();
    }
}
