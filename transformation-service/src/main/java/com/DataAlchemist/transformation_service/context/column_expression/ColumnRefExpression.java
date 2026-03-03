package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

public class ColumnRefExpression implements ColumnExpression{
    private final int index;

    public ColumnRefExpression(int index) {
        this.index = index;
    }

    @Override
    public Cell evaluate(Row row) {
        return row.getCells().get(index);
    }
}
