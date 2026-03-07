package com.DataAlchemist.transformation_service.context.pipe;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class FilterPipe implements Pipe{
    private final ColumnExpression columnExpression;

    public FilterPipe(ColumnExpression columnExpression) {
        this.columnExpression = columnExpression;
    }

    @Override
    public Row process(Row row) {
        Cell result = columnExpression.evaluate(row);
        if(result == null) throw new IllegalArgumentException("Unexpected argument, expected a boolean but found null");
        if(result.getColumnType() != ColumnType.BOOLEAN) throw new IllegalArgumentException("Unexpected argument, expected a boolean but found "+result.getColumnType());
        if(result.getValue() == null) throw new IllegalArgumentException("Unexpected argument, expression returned null");

        return Boolean.TRUE.equals(result.getValue()) ? row : null;
    }
}
