package com.DataAlchemist.transformation_service.context;

import com.DataAlchemist.transformation_service.models.enums.ColumnType;
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
    List<ColumnType> types = new ArrayList<>();

    public void addColumn(String column, String type) {
        columns.add(column);
        switch (type.toLowerCase()) {
                case "string" -> types.add(ColumnType.STRING);
                case "integer" -> types.add(ColumnType.INTEGER);
                case "double" -> types.add(ColumnType.DOUBLE);
                default -> types.add(ColumnType.DATE);
        };
    }
}
