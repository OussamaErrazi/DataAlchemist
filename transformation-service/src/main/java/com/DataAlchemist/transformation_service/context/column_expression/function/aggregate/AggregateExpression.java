package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

public abstract class AggregateExpression implements ColumnExpression {

    @Override
    public Cell evaluate(Row row) {
        throw new IllegalArgumentException();
    }

    public abstract void addRow(Row row);

    public abstract Cell get();

    public abstract AggregateExpression copy();
}
