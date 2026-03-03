package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

public class RenameColumnExpression implements ColumnExpression{
    private final int columnIndex;
    private final String newName;

    public RenameColumnExpression(int columnIndex, String newName) {
        this.columnIndex = columnIndex;
        this.newName = newName;
    }

    @Override
    public Cell evaluate(Row row) {
        row.getCells().get(columnIndex).setColumnName(newName);
        return row.getCells().get(columnIndex);
    }
}
