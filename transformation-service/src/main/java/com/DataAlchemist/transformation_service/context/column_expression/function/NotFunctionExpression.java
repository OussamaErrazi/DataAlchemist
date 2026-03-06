package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class NotFunctionExpression implements ColumnExpression {
    private final ColumnExpression condition;

    public NotFunctionExpression(ColumnExpression condition) {
        this.condition = condition;
    }

    @Override
    public Cell evaluate(Row row) {
        Cell result = condition.evaluate(row);
        if(result.getValue() == null) return Cell.builder().value(null).columnType(ColumnType.BOOLEAN).build();
        if(result.getColumnType() != ColumnType.BOOLEAN) {
            throw new IllegalArgumentException("Expected boolean but found "+result.getColumnType()+" in not function");
        }
        return Boolean.TRUE.equals(result.getValue()) ? Cell.builder().value(false).columnName("not "+result.getColumnName()).columnType(ColumnType.BOOLEAN).build() : Cell.builder().value(true).columnName("not "+result.getColumnName()).columnType(ColumnType.BOOLEAN).build();
    }
}
