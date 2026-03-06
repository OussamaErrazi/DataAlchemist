package com.DataAlchemist.transformation_service.context.column_expression.function.date_function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetYearFunctionExpression implements ColumnExpression {
    private final ColumnExpression input;

    public GetYearFunctionExpression(ColumnExpression input) {
        this.input = input;
    }

    @Override
    public Cell evaluate(Row row) {
        if(input == null) throw new IllegalArgumentException("Unexpected input, expected date input value in get_year function but found null");
        Cell inputDate = input.evaluate(row);
        if(inputDate.getColumnType() != ColumnType.DATE) throw new IllegalArgumentException("Expected a date in get_year function 1st arg but got "+inputDate.getColumnType());
        String datePattern = "^date\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher matcher = pattern.matcher(inputDate.getValue().toString());
        if(matcher.matches()) {
            Integer year = matcher.group(3).isEmpty() ? null : Integer.parseInt(matcher.group(3));
            return Cell.builder()
                    .value(year)
                    .columnType(ColumnType.INTEGER)
                    .columnName("get_year("+inputDate.getColumnName()+")")
                    .build();
        }
        try{
            LocalDate date = LocalDate.parse(inputDate.getValue().toString(), DateTimeFormatter.ofPattern("dd/MM/yyyy"));
            return Cell.builder()
                    .value(date.getYear())
                    .columnType(ColumnType.INTEGER)
                    .columnName("get_year("+inputDate.getColumnName()+")")
                    .build();
        }catch (Exception e) {
            throw new IllegalArgumentException("Invalid date format "+inputDate.getValue().toString()+" in get_year function");
        }
    }
}
