package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public interface ColumnExpression {

    Cell evaluate(Row row);

    static Cell getCellFromVal(Object val) {
        return Cell.builder()
                .value(val)
                .columnName(val.toString())
                .columnType(val instanceof Integer ? ColumnType.INTEGER : val instanceof String ? ColumnType.STRING : ColumnType.DOUBLE)
                .build();
    }
}
