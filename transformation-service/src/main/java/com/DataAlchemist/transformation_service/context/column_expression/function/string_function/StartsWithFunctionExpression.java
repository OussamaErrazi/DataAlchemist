package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class StartsWithFunctionExpression implements ColumnExpression {
    private final ColumnExpression word;
    private final ColumnExpression keyword;

    public StartsWithFunctionExpression(ColumnExpression word, ColumnExpression keyword) {
        this.word = word;
        this.keyword = keyword;
    }

    @Override
    public Cell evaluate(Row row) {
        if(word == null || keyword == null) throw new IllegalArgumentException("Unexpected input, expected string input value in starts_with function but found null");
        Cell wordString = word.evaluate(row);
        if(wordString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in starts_with function 1st arg but got "+wordString.getColumnType());
        Cell keywordString = keyword.evaluate(row);
        if(keywordString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in starts_with function 2nd arg but got "+keywordString.getColumnType());
        if(wordString.getValue() == null || keywordString.getValue() == null) return Cell.builder().value(null).columnType(ColumnType.BOOLEAN).columnName("starts_with("+wordString.getColumnName()+")").build();

        return Cell.builder().value(wordString.getValue().toString().startsWith(keywordString.getValue().toString())).columnType(ColumnType.BOOLEAN).columnName("starts_with("+wordString.getColumnName()+")").build();
    }
}
