package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

public class NullExpression implements ColumnExpression {



    @Override
    public Cell evaluate(Row row) {
        return Cell.builder()
                .value(null)
                .build();
    }
}
