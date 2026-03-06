package com.DataAlchemist.transformation_service.context.column_expression.function.date_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDayFunctionExpression implements ColumnExpression {
    private final ColumnExpression input;

    public GetDayFunctionExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public Cell evaluate(Row row) {
        if(input == null) throw new IllegalArgumentException("Unexpected input, expected date input value in get_day function but found null");
        Cell inputDate = input.evaluate(row);
        if(inputDate.getColumnType() != ColumnType.DATE) throw new IllegalArgumentException("Expected a date in get_day function 1st arg but got "+inputDate.getColumnType());
        String datePattern = "^date\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher matcher = pattern.matcher(inputDate.getValue().toString());
        if(matcher.matches()) {
            Integer day = matcher.group(1).isEmpty() ? null : Integer.parseInt(matcher.group(1));
            return Cell.builder()
                    .value(day)
                    .columnType(ColumnType.INTEGER)
                    .columnName("get_day("+inputDate.getColumnName()+")")
                    .build();
        }
        try{
            LocalDate date = LocalDate.parse(inputDate.getValue().toString(), DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            return Cell.builder()
                    .value(date.getDayOfMonth())
                    .columnType(ColumnType.INTEGER)
                    .columnName("get_day("+inputDate.getColumnName()+")")
                    .build();
        }catch (Exception e) {
            throw new IllegalArgumentException("Invalid date format "+inputDate.getValue().toString()+" in get_day function");
        }
    }
}
