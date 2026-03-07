package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class SumAggregateExpression extends AggregateExpression{
    private final ColumnExpression input;
    private double sum =0;
    private ColumnType type;
    private String name;

    public SumAggregateExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public void addRow(Row row) {
        Cell result = input.evaluate(row);
        if (result == null) throw new IllegalArgumentException(); //todo exception message
        if(!result.isNumeric()) throw new IllegalArgumentException(); //todo exception message
        type = result.getColumnType();
        name = result.getColumnName();
        sum += Double.parseDouble(result.getValue().toString());
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(sum)
                .columnType(type)
                .columnName("sum("+name+")")
                .build();
    }

    public AggregateExpression copy() {
        return new SumAggregateExpression(input);
    }
}
