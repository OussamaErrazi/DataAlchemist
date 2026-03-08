package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class AvgAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private double sum = 0;
    private int count = 0;
    private String name;

    public AvgAggregateExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public void addRow(Row row) {
        Cell result = input.evaluate(row);
        if (result == null) throw new IllegalArgumentException(); //todo exception message
        if(!result.isNumeric()) throw new IllegalArgumentException(); //todo exception message
        name = result.getColumnName();
        sum += Double.parseDouble(result.getValue().toString());
        count++;
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(count==0 ? null : sum/count)
                .columnType(ColumnType.DOUBLE)
                .columnName("avg("+name+")")
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new AvgAggregateExpression(input);
    }
}
