package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class ContainsFunctionExpression implements ColumnExpression {
    private final ColumnExpression inputString;
    private final ColumnExpression containsString;

    public ContainsFunctionExpression(ColumnExpression inputString, ColumnExpression containsString) {
        this.inputString = inputString;
        this.containsString = containsString;
    }

    @Override
    public Cell evaluate(Row row) {
        Cell input = inputString.evaluate(row);
        if(input.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in contains function 1st arg but got "+input.getColumnType());
        Cell contains = containsString.evaluate(row);
        if(contains.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a contains in trim function 2nd arg but got "+contains.getColumnType());
        if(input.getValue() == null) return input;
        if(contains.getValue() == null) return contains;
        return Cell.builder()
                .columnType(ColumnType.BOOLEAN)
                .columnName(input.getColumnName()+" contains "+contains.getColumnName())
                .value(input.getValue().toString().contains(contains.getValue().toString()))
                .build();
    }
}
