package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class SubstringFunctionExpression implements ColumnExpression {
    private final ColumnExpression inputString;
    private final ColumnExpression from;
    private final ColumnExpression to;

    public SubstringFunctionExpression(ColumnExpression inputString, ColumnExpression from, ColumnExpression columnExpression) {
        this.inputString = inputString;
        this.from = from;
        to = columnExpression;
    }

    @Override
    public Cell evaluate(Row row) {
        if (inputString == null || from == null) throw new IllegalArgumentException("Unexpected input, expected string input value but found null");
        Cell input = inputString.evaluate(row);
        if(input.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in substring function 1st arg but got "+input.getColumnType());
        Cell fromI = from.evaluate(row);
        if(fromI.getColumnType() != ColumnType.INTEGER) throw new IllegalArgumentException("Expected an integer in substring function 2nd arg but got "+fromI.getColumnType());
        if(input.getValue() == null || fromI.getValue() == null)
            return Cell.builder()
                    .value(null)
                    .columnType(ColumnType.STRING)
                    .columnName(input.getColumnName())
                    .build();
        if(to == null) {
            input.setValue(input.getValue().toString().substring((Integer) fromI.getValue()));
            return input;
        }
        Cell toI = to.evaluate(row);
        if(toI.getColumnType() != ColumnType.INTEGER) throw new IllegalArgumentException("Expected an integer in substring function 3rd arg but got "+toI.getColumnType());
        if(toI.getValue() == null) {
            return Cell.builder()
                    .value(null)
                    .columnType(ColumnType.STRING)
                    .columnName(input.getColumnName())
                    .build();
        }
        input.setValue(input.getValue().toString().substring((Integer) fromI.getValue(), (Integer) toI.getValue()));
        return input;
    }
}
