package com.DataAlchemist.transformation_service.context.column_expression;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface ColumnExpressionResolver {

    static ColumnExpression resolve(String expr){
        expr = expr.trim();
        Pattern pattern;
        if(expr.matches("%[1-9][0-9]*")){
            return new ColumnRefExpression(Integer.parseInt(expr.substring(1))-1);
        }
        String arithmeticOpPattern = "^%([1-9][0-9]*) *([+\\-/*]) *%([1-9][0-9]*)$";
        pattern = Pattern.compile(arithmeticOpPattern);
        Matcher arithmeticOpPatternMatcher = pattern.matcher(expr);
        if(arithmeticOpPatternMatcher.matches()) {
            return new ArithmeticOpExpression(
                    arithmeticOpPatternMatcher.group(2).charAt(0),
                    Integer.parseInt(arithmeticOpPatternMatcher.group(1))-1,
                    Integer.parseInt(arithmeticOpPatternMatcher.group(3))-1
            );
        }

        String renamingOpPattern = "^%([1-9][0-9]*) +as +\"([%a-z0-9A-Z-_. $&/]+)\"$";
        pattern = Pattern.compile(renamingOpPattern);
        Matcher renamingOpPatternMatcher = pattern.matcher(expr);
        if(renamingOpPatternMatcher.matches()) {
            return new RenameColumnExpression(Integer.parseInt(renamingOpPatternMatcher.group(1))-1, renamingOpPatternMatcher.group(2));
        }
        throw new IllegalArgumentException("Cannot Resolve Expression : "+expr);
    }
}
