package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class LowerFunctionExpression implements ColumnExpression {
    private final ColumnExpression inputString;

    public LowerFunctionExpression(ColumnExpression inputString) {
        this.inputString = inputString;
    }

    @Override
    public Cell evaluate(Row row) {
        if(inputString == null) throw new IllegalArgumentException("Unexpected input, expected string input value in lower function but found null");
        Cell result = inputString.evaluate(row);
        if(result.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in lower function but got "+result.getColumnType());
        if(result.getValue() == null) return result;
        result.setValue(result.getValue().toString().toLowerCase());
        return result;
    }
}
