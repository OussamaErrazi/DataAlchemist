package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class DateFunctionExpression implements ColumnExpression {
    private final ColumnExpression day;
    private final ColumnExpression month;
    private final ColumnExpression year;

    public DateFunctionExpression(ColumnExpression day, ColumnExpression month, ColumnExpression year) {
        this.day = day;
        this.month = month;
        this.year = year;
    }


    @Override
    public Cell evaluate(Row row) {
        String d = day==null ? "" : day.evaluate(row).getValue().toString();
        String m = month==null ? "" : month.evaluate(row).getValue().toString();
        String y = year==null ? "" : year.evaluate(row).getValue().toString();
        return Cell.builder()
                .value("date("+d+","+m+","+y+")")
                .columnName("date")
                .columnType(ColumnType.DATE)
                .build();
    }
}
