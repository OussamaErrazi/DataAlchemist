package com.DataAlchemist.transformation_service.models;


import com.DataAlchemist.transformation_service.models.enums.ColumnType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Cell {

    private Object value;
    private String columnName;
    private ColumnType columnType;

    public boolean isNumeric() {
        return columnType == ColumnType.INTEGER || columnType == ColumnType.DOUBLE;
    }
}
