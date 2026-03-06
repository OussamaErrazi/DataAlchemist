package com.DataAlchemist.transformation_service.context.column_expression.function.string_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class ReplaceFunctionExpression implements ColumnExpression {
    private final ColumnExpression word;
    private final ColumnExpression target;
    private final ColumnExpression replaceWith;

    public ReplaceFunctionExpression(ColumnExpression word, ColumnExpression target, ColumnExpression replaceWith) {
        this.word = word;
        this.target = target;
        this.replaceWith = replaceWith;
    }


    @Override
    public Cell evaluate(Row row) {
        if(word == null || target == null || replaceWith == null) throw new IllegalArgumentException("Unexpected input, expected string input value in replace function but found null");
        Cell wordString = word.evaluate(row);
        if(wordString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in replace function 1st arg but got "+wordString.getColumnType());
        Cell targetString = target.evaluate(row);
        if(targetString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in replace function 2nd arg but got "+targetString.getColumnType());
        Cell replaceWithString = replaceWith.evaluate(row);
        if(replaceWithString.getColumnType() != ColumnType.STRING) throw new IllegalArgumentException("Expected a string in replace function 3rd arg but got "+replaceWithString.getColumnType());

        if(wordString.getValue() == null || targetString.getValue() == null || replaceWithString.getValue() == null)
            return Cell.builder()
                    .value(null)
                    .columnType(ColumnType.STRING)
                    .columnName("replace("+wordString.getColumnName()+")")
                    .build();
        return Cell.builder()
                .value(wordString.getValue().toString().replace(targetString.getValue().toString(), replaceWithString.getValue().toString()))
                .columnType(ColumnType.STRING)
                .columnName("replace("+wordString.getColumnName()+")")
                .build();
    }
}
