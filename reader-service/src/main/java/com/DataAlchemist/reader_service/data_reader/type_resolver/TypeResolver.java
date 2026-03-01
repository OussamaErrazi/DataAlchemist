package com.DataAlchemist.reader_service.data_reader.type_resolver;

import java.util.List;

public interface TypeResolver {
    Class<?> resolveType(List<String> values);
}
