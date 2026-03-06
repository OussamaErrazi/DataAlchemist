package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class IndexOfFunctionExpression implements ColumnExpression {
    private final ColumnExpression input;
    private final ColumnExpression keyword;

    public IndexOfFunctionExpression(ColumnExpression input, ColumnExpression keyword) {
        this.input = input;
        this.keyword = keyword;
    }


    @Override
    public Cell evaluate(Row row) {
        if (input == null || keyword == null) throw new IllegalArgumentException("Unexpected input, expected string input value but found null");
        Cell inputString = input.evaluate(row);
        if(inputString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in index_of function 1st arg but got "+inputString.getColumnType());
        Cell keywordString = keyword.evaluate(row);
        if(keywordString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in index_of function 2nd arg but got "+keywordString.getColumnType());
        if(inputString.getValue() == null || keywordString.getValue() == null)
            return Cell.builder()
                    .value(null)
                    .columnType(ColumnType.STRING)
                    .columnName(inputString.getColumnName())
                    .build();
        return Cell.builder()
                .value(inputString.getValue().toString().indexOf(keywordString.getValue().toString()))
                .columnName("index of "+inputString.getColumnName())
                .columnType(ColumnType.INTEGER)
                .build();
    }
}
