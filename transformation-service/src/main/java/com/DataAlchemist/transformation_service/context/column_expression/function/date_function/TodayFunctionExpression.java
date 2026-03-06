package com.DataAlchemist.transformation_service.context.column_expression.function.date_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class TodayFunctionExpression implements ColumnExpression {
    @Override
    public Cell evaluate(Row row) {
        LocalDate date = LocalDate.now();
        return Cell.builder()
                .value(date.format(DateTimeFormatter.ofPattern("dd/MM/yyyy")))
                .columnName("today")
                .columnType(ColumnType.DATE)
                .build();
    }
}
