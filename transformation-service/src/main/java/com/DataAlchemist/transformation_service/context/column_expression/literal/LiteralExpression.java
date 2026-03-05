package com.DataAlchemist.transformation_service.context.column_expression.literal;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class LiteralExpression implements ColumnExpression {
    private final Object value;

    public LiteralExpression(Object value) {
        this.value = value;
    }

    @Override
    public Cell evaluate(Row row) {
        if (value instanceof Integer) {
            return Cell.builder()
                    .value(value)
                    .columnName(value.toString())
                    .columnType(ColumnType.INTEGER)
                    .build();
        }
        if (value instanceof Double)
            return Cell.builder()
                    .value(value)
                    .columnName(value.toString())
                    .columnType(ColumnType.DOUBLE)
                    .build();
        if (value instanceof String)
            return Cell.builder()
                    .value(value)
                    .columnName(value.toString())
                    .columnType(ColumnType.STRING)
                    .build();
        if (value instanceof Boolean)
            return Cell.builder()
                    .value(value)
                    .columnName(value.toString())
                    .columnType(ColumnType.BOOLEAN)
                    .build();
        return null;
    }
}
