package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class CountAggregateExpression extends AggregateExpression{
    private long count=0;


    @Override
    public void addRow(Row row) {
        count++;
    }

    @Override
    public Cell get() {
        return Cell.builder()
                .value(count)
                .columnName("count()")
                .columnType(ColumnType.INTEGER)
                .build();
    }

    @Override
    public AggregateExpression copy() {
        return new CountAggregateExpression();
    }
}
