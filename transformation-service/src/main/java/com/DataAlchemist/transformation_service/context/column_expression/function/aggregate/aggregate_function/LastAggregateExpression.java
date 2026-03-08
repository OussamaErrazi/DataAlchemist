package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class LastAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private Object val = null;
    private ColumnType type = null;
    private String name = null;

    public LastAggregateExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public void addRow(Row row) {
        if(input == null) throw new IllegalArgumentException(); //todo exception message
        Cell result = input.evaluate(row);
        val = result.getValue();
        type = result.getColumnType();
        name = result.getColumnName();
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(val)
                .columnType(type)
                .columnName("last("+name+")")
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new LastAggregateExpression(input);
    }
}
