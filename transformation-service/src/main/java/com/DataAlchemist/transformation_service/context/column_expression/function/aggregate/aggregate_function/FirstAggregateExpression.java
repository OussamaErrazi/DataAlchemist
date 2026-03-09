package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class FirstAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private Object val = null;
    private ColumnType type = null;
    private String name = null;

    public FirstAggregateExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public void addRow(Row row) {
        if(val != null) return;
        if(input == null) throw new IllegalArgumentException(); //todo exception message
        Cell result = input.evaluate(row);
        if(val == null) {
            val = result.getValue();
            type = result.getColumnType();
            name = result.getColumnName();
        }
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(val)
                .columnType(type)
                .columnName("first("+name+")")
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new FirstAggregateExpression(input);
    }
}
