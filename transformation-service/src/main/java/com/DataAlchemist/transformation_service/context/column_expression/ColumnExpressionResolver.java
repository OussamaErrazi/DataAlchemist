package com.DataAlchemist.transformation_service.context.column_expression;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface ColumnExpressionResolver {

    static ColumnExpression resolve(String expr){
        expr = expr.trim();
        if(expr.matches("%[1-9][0-9]*")){
            return new ColumnRefExpression(Integer.parseInt(expr.substring(1))-1);
        }
        String arithmeticOpPattern = "^%([1-9][0-9]*) *([+\\-/*]) *%([1-9][0-9]*)$";
        Pattern pattern = Pattern.compile(arithmeticOpPattern);
        Matcher arithmeticOpPatternMatcher = pattern.matcher(expr);
        if(arithmeticOpPatternMatcher.matches()) {
            return new ArithmeticOpExpression(
                    arithmeticOpPatternMatcher.group(2).charAt(0),
                    Integer.parseInt(arithmeticOpPatternMatcher.group(1)),
                    Integer.parseInt(arithmeticOpPatternMatcher.group(3))
            );
        }
        throw new IllegalArgumentException("Cannot Resolve Expression : "+expr);
    }
}
