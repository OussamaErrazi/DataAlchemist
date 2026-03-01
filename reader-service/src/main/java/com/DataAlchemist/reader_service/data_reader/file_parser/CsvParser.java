package com.DataAlchemist.reader_service.data_reader.file_parser;

import com.DataAlchemist.reader_service.constants.AppConstants;
import com.DataAlchemist.reader_service.data_reader.type_resolver.DefaultTypeResolver;
import com.DataAlchemist.reader_service.data_reader.type_resolver.TypeResolver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Stream;

public class CsvParser implements FileParser{
    private final TypeResolver typeResolver;

    public CsvParser() {
        this.typeResolver = new DefaultTypeResolver();
    }

    // only for future use cases to add more data types retrieving logic, ex: boolean, characters ...
    public CsvParser(TypeResolver typeResolver) {
        this.typeResolver = typeResolver;
    }


    @Override
    public Map<String, Class<?>> getSchema(InputStream inputStream) throws IOException {
        Map<String, Class<?>> schema = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String headerLine = reader.readLine();
            if(headerLine == null) throw new RuntimeException("CSV file is empty");

            String[] headers = headerLine.split(AppConstants.CSV_LINE_SPLITTER);
            List<List<String>> columnSampleValues = new ArrayList<>();
            for(int i=0; i<headers.length; i++) columnSampleValues.add(new ArrayList<>());

            String line;
            int count = 0;

            while((line = reader.readLine()) != null && count < AppConstants.SAMPLE_LINES) {
                String[] values = line.split(AppConstants.CSV_LINE_SPLITTER);
                for(int i =0; i< headers.length && i< values.length; i++) columnSampleValues.get(i).add(values[i]);
                count++;
            }

            for(int i=0; i< headers.length; i++) {
                schema.put(headers[i], typeResolver.resolveType(columnSampleValues.get(i)));
            }
        }
        return schema;
    }

    @Override
    public Stream<Map<String, Object>> parse(InputStream inputStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String header = reader.readLine();
        if (header == null) throw new IOException("CSV file is empty");
        String[] headers = header.split(AppConstants.CSV_LINE_SPLITTER);

        return reader.lines().map(line -> {
            String[] values = line.split(AppConstants.CSV_LINE_SPLITTER);
            if(values.length != headers.length) throw new RuntimeException("row violates schema format. "+line);
            Map<String, Object> row = new LinkedHashMap<>();
            for(int i=0; i<headers.length; i++) {
                String fieldName = headers[i].trim();
                row.put(fieldName, values[i]);
            }
            return row;
        });
    }
}
