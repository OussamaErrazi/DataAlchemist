package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

public class DefaultFunctionExpression implements ColumnExpression {
    private final ColumnExpression column;
    private final ColumnExpression fallback_value;

    public DefaultFunctionExpression(ColumnExpression column, ColumnExpression fallback_value) {
        this.column = column;
        this.fallback_value = fallback_value;
    }

    @Override
    public Cell evaluate(Row row) {
        Cell value = column.evaluate(row);
        Cell fallback = fallback_value.evaluate(row);
        if(value.getColumnType() !=null && fallback.getColumnType() !=null && !value.getColumnType().equals(fallback.getColumnType())) {
            throw new IllegalArgumentException("Default value mismatch. Expected "+value.getColumnType()+" but default value is of type "+fallback.getColumnType());
        }
        fallback.setColumnName(value.getColumnName());
        return value.getValue() == null ? fallback : value;
    }
}
