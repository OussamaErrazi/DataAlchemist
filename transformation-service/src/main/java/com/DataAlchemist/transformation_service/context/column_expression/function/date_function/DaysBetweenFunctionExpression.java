package com.DataAlchemist.transformation_service.context.column_expression.function.date_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DaysBetweenFunctionExpression implements ColumnExpression {
    private final ColumnExpression date1;
    private final ColumnExpression date2;

    public DaysBetweenFunctionExpression(ColumnExpression date1, ColumnExpression date2) {
        this.date1 = date1;
        this.date2 = date2;
    }

    @Override
    public Cell evaluate(Row row) {
        if(date1 == null || date2 == null) throw new IllegalArgumentException("Unexpected input, expected date input value in days_between function but found null");
        Cell date1Value = date1.evaluate(row);
        if(date1Value.getColumnType() != ColumnType.DATE) throw new IllegalArgumentException("Expected a date in days_between function 1st arg but got "+date1Value.getColumnType());

        Cell date2Value = date2.evaluate(row);
        if(date2Value.getColumnType() != ColumnType.DATE) throw new IllegalArgumentException("Expected a date in days_between function 2nd arg but got "+date2Value.getColumnType());
        if(date1Value.getValue() == null || date2Value.getValue() == null)
            return Cell.builder()
                    .value(null)
                    .columnType(ColumnType.INTEGER)
                    .columnName("days_between("+date1Value.getColumnName()+","+date2Value.getColumnName()+")")
                    .build();

        int day1 = DateStringParser.getDay(date1Value.getValue().toString()),
                day2 = DateStringParser.getDay(date2Value.getValue().toString());
        int month1 = DateStringParser.getMonth(date1Value.getValue().toString()),
                month2 = DateStringParser.getMonth(date2Value.getValue().toString());
        int year1 = DateStringParser.getYear(date1Value.getValue().toString()),
                year2 = DateStringParser.getYear(date2Value.getValue().toString());

        if(day1 == 0) {
            day1 = day2;
        }
        if(day2 == 0) {
            day2 = day1;
        }
        if(month1 == 0) {
            month1 = month2;
        }
        if(month2 == 0) {
            month2 = month1;
        }
        if(year1 == 0) {
            year1 = year2;
        }
        if(year2 == 0) {
            year2 = year1;
        }

        if(day1 == 0) {
            day1=1;day2=1;
        }
        if(month1 == 0) {
            month1=1; month2=1;
        }
        if(year1==0) {
            year1=1; year2=1;
        }
        return Cell.builder()
                .value(ChronoUnit.DAYS.between(LocalDate.of(year1, month1, day1), LocalDate.of(year2, month2, day2)))
                .columnType(ColumnType.INTEGER)
                .columnName("days_between("+date1Value.getColumnName()+","+date2Value.getColumnName()+")")
                .build();
    }


}
