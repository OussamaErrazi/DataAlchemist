package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class MinAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private double min = Double.MAX_VALUE;
    private ColumnType type;
    private String name;

    public MinAggregateExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public void addRow(Row row) {
        Cell result = input.evaluate(row);
        if (result == null) throw new IllegalArgumentException(); //todo exception message
        if(!result.isNumeric()) throw new IllegalArgumentException(); //todo exception message
        type = result.getColumnType();
        name = result.getColumnName();
        min = Math.min(Double.parseDouble(result.getValue().toString()), min);
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(type == ColumnType.DOUBLE ? min : (long) min)
                .columnType(type)
                .columnName("min("+name+")")
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new MinAggregateExpression(input);
    }
}
