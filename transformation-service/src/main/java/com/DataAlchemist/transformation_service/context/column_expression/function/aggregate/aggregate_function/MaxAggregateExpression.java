package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class MaxAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private double max = Double.MIN_VALUE;
    private ColumnType type;
    private String name;

    public MaxAggregateExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public void addRow(Row row) {
        Cell result = input.evaluate(row);
        if (result == null) throw new IllegalArgumentException(); //todo exception message
        if(!result.isNumeric()) throw new IllegalArgumentException(); //todo exception message
        type = result.getColumnType();
        name = result.getColumnName();
        max = Math.max(Double.parseDouble(result.getValue().toString()), max);
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(type == ColumnType.DOUBLE ? max : (long) max)
                .columnType(type)
                .columnName("max("+name+")")
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new MaxAggregateExpression(input);
    }
}
