package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class DateLiteralExpression implements ColumnExpression{
    private final String date;

    public DateLiteralExpression(String date) {
        this.date = date;
    }


    @Override
    public Cell evaluate(Row row) {
        return Cell.builder()
                .value(date)
                .columnName(date)
                .columnType(ColumnType.DATE)
                .build();
    }
}
