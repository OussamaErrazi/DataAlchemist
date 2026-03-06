package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class LengthFunctionExpression implements ColumnExpression {
    private final ColumnExpression inputString;

    public LengthFunctionExpression(ColumnExpression inputString) {
        this.inputString = inputString;
    }

    @Override
    public Cell evaluate(Row row) {
        Cell result = inputString.evaluate(row);
        if(result.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in trim function but got "+result.getColumnType());
        if(result.getValue() == null) return result;
        result.setValue(result.getValue().toString().trim());
        return Cell.builder()
                .value(result.getValue().toString().length())
                .columnName("length("+result.getColumnName()+")")
                .columnType(ColumnType.INTEGER)
                .build();
    }
}
