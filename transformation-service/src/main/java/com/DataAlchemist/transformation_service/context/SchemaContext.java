package com.DataAlchemist.transformation_service.context;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SchemaContext {
    List<String> columns = new ArrayList<>();
    List<String> types = new ArrayList<>();

    public void addColumn(String column, String type) {
        columns.add(column);
        types.add(type);
    }
}
