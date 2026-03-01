package com.DataAlchemist.reader_service.data_reader.file_parser;

import com.DataAlchemist.reader_service.constants.AppConstants;
import com.DataAlchemist.reader_service.data_reader.type_resolver.DefaultTypeResolver;
import com.DataAlchemist.reader_service.data_reader.type_resolver.TypeResolver;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.MappingIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JsonParser implements FileParser{
    private final ObjectMapper mapper = new ObjectMapper();
    private final TypeResolver typeResolver;

    public JsonParser() {
        this.typeResolver = new DefaultTypeResolver();
    }

    public JsonParser(TypeResolver typeResolver) {
        this.typeResolver = typeResolver;
    }

    @Override
    public Map<String, Class<?>> getSchema(InputStream inputStream) throws IOException {
        Map<String, List<String>> samples = new LinkedHashMap<>();

        try (com. fasterxml. jackson. core. JsonParser jp = new JsonFactory().createParser(inputStream)) {
            if (jp.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalArgumentException("Expected JSON array at root");
            }

            int count = 0;
            while (jp.nextToken() == JsonToken.START_OBJECT && count < AppConstants.SAMPLE_LINES) {
                Map<String, String> obj = mapper.readValue(jp, new TypeReference<Map<String, String>>() {});
                obj.forEach((k, v) -> samples.computeIfAbsent(k, key -> new ArrayList<>()).add(v));
                count++;
            }
        }


        Map<String, Class<?>> schema = new LinkedHashMap<>();
        samples.forEach((field, values) -> schema.put(field, typeResolver.resolveType(values)));
        return schema;
    }

    @Override
    public Stream<Map<String, Object>> parse(InputStream inputStream) throws IOException {
        com.fasterxml.jackson.core.JsonParser jp = new JsonFactory().createParser(inputStream);

        if (jp.nextToken() != JsonToken.START_ARRAY) {
            throw new IllegalArgumentException("Expected JSON array at root");
        }

        Iterator<Map<String, Object>> iterator = new Iterator<>() {
            private Map<String, Object> nextItem = readNext();

            private Map<String, Object> readNext() {
                try {
                    JsonToken token = jp.nextToken();
                    if (token == JsonToken.END_ARRAY || token == null) {
                        return null;
                    }
                    // Read the current object (parser is positioned at START_OBJECT)
                    Map<String, Object> raw = mapper.readValue(jp, Map.class);
                    return new LinkedHashMap<>(raw);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return nextItem != null;
            }

            @Override
            public Map<String, Object> next() {
                if (nextItem == null) {
                    throw new NoSuchElementException();
                }
                Map<String, Object> current = nextItem;
                nextItem = readNext();
                return current;
            }
        };

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, 0),
                false
        ).onClose(() -> {
            try {
                jp.close();
                inputStream.close();
            } catch (IOException ignored) {}
        });
    }
}
