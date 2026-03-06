package com.DataAlchemist.transformation_service.context.column_expression.function.date_function;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface DateStringParser {


    static int getDay(String date){
        int day;
        String datePattern = "^date\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher matcher1 = pattern.matcher(date);
        if(matcher1.matches()) {
            day = matcher1.group(1).isEmpty() ? 0 : Integer.parseInt(matcher1.group(1));
        } else {
            try {
                day = LocalDate.parse(date, DateTimeFormatter.ofPattern("dd/MM/yyyy")).getDayOfMonth();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown date format in days_between function "+date);
            }
        }
        return day;
    }

    static int getMonth(String date){
        int month;
        String datePattern = "^date\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher matcher1 = pattern.matcher(date);
        if(matcher1.matches()) {
            month = matcher1.group(2).isEmpty() ? 0 : Integer.parseInt(matcher1.group(2));
        } else {
            try {
                month = LocalDate.parse(date, DateTimeFormatter.ofPattern("dd/MM/yyyy")).getMonthValue();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown date format in months_between function "+date);
            }
        }
        return month;
    }

    static int getYear(String date){
        int year;
        String datePattern = "^date\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher matcher1 = pattern.matcher(date);
        if(matcher1.matches()) {
            year = matcher1.group(3).isEmpty() ? 0 : Integer.parseInt(matcher1.group(3));
        } else {
            try {
                year = LocalDate.parse(date, DateTimeFormatter.ofPattern("dd/MM/yyyy")).getYear();
            } catch (Exception e) {
                throw new IllegalArgumentException("Unknown date format in years_between function "+date);
            }
        }
        return year;
    }
}
