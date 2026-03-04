package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ComparisonExpression implements ColumnExpression{
    private final String op;
    private final ColumnExpression leftOperand;
    private final ColumnExpression rightOperand;

    public ComparisonExpression(String op, ColumnExpression leftOperand, ColumnExpression rightOperand) {
        this.op = op;
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
    }


    @Override
    public Cell evaluate(Row row) {
        Cell left = leftOperand.evaluate(row);
        Cell right = rightOperand.evaluate(row);
        if(left.isNumeric() != right.isNumeric()) {
            throw new IllegalArgumentException("Unsupported operation '"+op+"' between "+left.getValue()+" with type "+left.getColumnType()+" and "+right.getValue()+" with type "+right.getColumnType());
        }
        if(left.getColumnType() != right.getColumnType() && !left.isNumeric()) {
            throw new IllegalArgumentException("Unsupported operation '"+op+"' between "+left.getValue()+" with type "+left.getColumnType()+" and "+right.getValue()+" with type "+right.getColumnType());
        }
        switch (op) {
            case "==" : return eq(left, right);
            case "!=" : {Cell res = eq(left, right); res.setValue(!Boolean.parseBoolean(res.getValue().toString())); return res;}
            case "<" : return lt(left, right);
            case ">" : return lt(right, left);
            case "<=" : {Cell resLt = lt(left, right); Cell resEq = eq(left, right); return Cell.builder().columnType(ColumnType.BOOLEAN).value(Boolean.parseBoolean(resLt.getValue().toString()) || Boolean.parseBoolean(resEq.getValue().toString())).build();}
            case ">=" : {Cell resGt = lt(right, left); Cell resEq = eq(left, right); return Cell.builder().columnType(ColumnType.BOOLEAN).value(Boolean.parseBoolean(resGt.getValue().toString()) || Boolean.parseBoolean(resEq.getValue().toString())).build();}

        }
        return null;
    }

    private Cell eq(Cell val1, Cell val2) {
        return switch(val1.getColumnType()) {
            case ColumnType.DOUBLE, ColumnType.INTEGER ->
                Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(Double.parseDouble(val1.getValue().toString()) == Double.parseDouble(val2.getValue().toString()))
                        .build();
            case ColumnType.DATE ->
                Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(compareDates("==", val1.getValue().toString(), val2.getValue().toString()))
                        .build();
            case ColumnType.STRING ->
                Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(val1.getValue().toString().equals(val2.getValue().toString()))
                        .build();
            case ColumnType.BOOLEAN ->
                Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(Boolean.parseBoolean(val1.getValue().toString()) == Boolean.parseBoolean(val2.getValue().toString()))
                        .build();
        };
    }

    private Cell lt(Cell val1, Cell val2) {
        switch(val1.getColumnType()) {
            case ColumnType.DOUBLE, ColumnType.INTEGER -> {
                return Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(Double.parseDouble(val1.getValue().toString()) < Double.parseDouble(val2.getValue().toString()))
                        .build();
            }
            case ColumnType.DATE -> {
                return Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(compareDates("<", val1.getValue().toString(), val2.getValue().toString()))
                        .build();
            }
            case ColumnType.STRING -> {
                return Cell.builder()
                        .columnType(ColumnType.BOOLEAN)
                        .value(val1.getValue().toString().compareTo(val2.getValue().toString()) < 0)
                        .build();
            }
            default -> throw new IllegalArgumentException("unsupported operation '<' between two "+val1.getColumnType());
        }
    }

    private boolean compareDates(String o, String date1, String date2) {
        int day1, month1, year1;
        int day2, month2, year2;
        String datePattern = "^date\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher matcher1 = pattern.matcher(date1);
        if(matcher1.matches()){
            day1 = matcher1.group(1).isEmpty() ? -1 : Integer.parseInt(matcher1.group(1));
            month1 = matcher1.group(2).isEmpty() ? -1 : Integer.parseInt(matcher1.group(2));
            year1 = matcher1.group(3).isEmpty() ? -1 : Integer.parseInt(matcher1.group(3));
        } else {
            try {
                LocalDate d1 = LocalDate.parse(date1, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
                day1 = d1.getDayOfMonth();
                month1 = d1.getMonthValue();
                year1 = d1.getYear();
            } catch (Exception e){
                throw new IllegalArgumentException("Unknown date format "+date1);
            }
        }

        Matcher matcher2 = pattern.matcher(date2);
        if(matcher2.matches()){
            day2 = matcher2.group(1).isEmpty() ? -1 : Integer.parseInt(matcher2.group(1));
            month2 = matcher2.group(2).isEmpty() ? -1 : Integer.parseInt(matcher2.group(2));
            year2 = matcher2.group(3).isEmpty() ? -1 : Integer.parseInt(matcher2.group(3));
        } else {
            try {
                LocalDate d2 = LocalDate.parse(date2, DateTimeFormatter.ofPattern("dd/MM/yyyy"));
                day2 = d2.getDayOfMonth();
                month2 = d2.getMonthValue();
                year2 = d2.getYear();
            } catch (Exception e){
                throw new IllegalArgumentException("Unknown date format "+date2);
            }
        }
        if(day1 == -1 || day2 == -1) {
            day1 = -1; day2 =-1;
        }
        if(month1 == -1 || month2 == -1) {
            month1 = -1; month2 = -1;
        }
        if(year1 == -1 || year2 == -1) {
            year1 = -1; year2 = -1;
        }

        return switch (o) {
            case "==" -> day1 == day2 && month1 == month2 && year1 == year2;
            case "<" -> year1<year2 || (year1==year2 && month1<month2) || (year1==year2 && month1==month2 && day1<day2);
            default -> throw new IllegalArgumentException("Unsupported comparison operation "+o);
        };
    }
}
