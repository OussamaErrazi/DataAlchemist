package com.DataAlchemist.transformation_service.context.pipe;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Row;

import java.util.ArrayList;
import java.util.List;

public class TransformationPipe implements Pipe {

    private final List<ColumnExpression> columnExpressions = new ArrayList<>();

    public void addColumnExpression(ColumnExpression expr) {
        columnExpressions.add(expr);
    }

    @Override
    public Row process(Row row) {
        Row newRow = new Row();
        for(ColumnExpression expression : columnExpressions) {
            newRow.addCell(expression.evaluate(row));
        }
        return newRow;
    }
}
