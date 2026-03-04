package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

public class RenameColumnExpression implements ColumnExpression{
    private final ColumnExpression column;
    private final String newName;

    public RenameColumnExpression(ColumnExpression column, String newName) {
        this.column = column;
        this.newName = newName;
    }

    @Override
    public Cell evaluate(Row row) {
        Cell result = column.evaluate(row);
        result.setColumnName(newName);
        return result;
    }
}
