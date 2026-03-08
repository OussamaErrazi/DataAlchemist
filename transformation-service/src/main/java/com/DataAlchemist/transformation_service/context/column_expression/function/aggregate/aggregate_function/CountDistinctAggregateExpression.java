package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.aggregate_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.util.HashSet;
import java.util.Set;

public class CountDistinctAggregateExpression extends AggregateExpression {
    private final ColumnExpression input;
    private final Set<Object> objects = new HashSet<>();

    public CountDistinctAggregateExpression(ColumnExpression input) {
        this.input = input;
    }


    @Override
    public void addRow(Row row) {
        Cell res = input.evaluate(row);
        if(res==null) throw new IllegalArgumentException();
        objects.add(res.getValue());
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(objects.size())
                .columnName("count_distinct()")
                .columnType(ColumnType.INTEGER)
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new CountDistinctAggregateExpression(input);
    }
}
