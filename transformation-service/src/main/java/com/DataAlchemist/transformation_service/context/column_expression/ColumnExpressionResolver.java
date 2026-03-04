package com.DataAlchemist.transformation_service.context.column_expression;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public interface ColumnExpressionResolver {

    static ColumnExpression resolve(String expr){
//        expr = expr.trim();
//        Pattern pattern;
//        if(expr.matches("%[1-9][0-9]*")){
//            return new ColumnRefExpression(Integer.parseInt(expr.substring(1))-1);
//        }
//        //TODO change here the arith pattern to include raw values not only between columns
//        // using this regex
//        String arithmeticOpPattern =  "^(\"?[\"-_.,% 0-9a-zA-Z]*\"?) *([+\\-/*]) *(\"?[\"-_.,% 0-9a-zA-Z]*\"?)$";
////        String arithmeticOpPattern = "^%([1-9][0-9]*) *([+\\-/*]) *%([1-9][0-9]*)$";
//        pattern = Pattern.compile(arithmeticOpPattern);
//        Matcher arithmeticOpPatternMatcher = pattern.matcher(expr);
//        if(arithmeticOpPatternMatcher.matches()) {
//            char op = arithmeticOpPatternMatcher.group(2).charAt(0);
//            String left=arithmeticOpPatternMatcher.group(1).trim();
//            String right=arithmeticOpPatternMatcher.group(3).trim();
//            int leftOperand=-1;
//            int rightOperand=-1;
//            Object leftVal = null, rightVal = null;
//
//            if(left.matches("%[1-9][0-9]*")) leftOperand = Integer.parseInt(left.substring(1))-1;
//            else if(left.matches("[0-9]+")) {
//                leftVal = Integer.parseInt(left);
//            } else if (left.matches("[0-9]+\\.[0-9]*")) {
//                leftVal = Double.parseDouble(left);
//            } else if (left.matches("\".*\"")) {
//                leftVal = left.substring(1, left.length()-1);
//            }
//
//            if(right.matches("%[1-9][0-9]*")) rightOperand = Integer.parseInt(right.substring(1))-1;
//            else if(right.matches("[0-9]+")) {
//                rightVal = Integer.parseInt(right);
//            } else if (right.matches("[0-9]+\\.[0-9]*")) {
//                rightVal = Double.parseDouble(right);
//            } else if (right.matches("\".*\"")) {
//                rightVal = right.substring(1, right.length()-1);
//            }
//            return new ArithmeticOpExpression(
//                    op,
//                    leftOperand, leftVal,
//                    rightOperand, rightVal
//            );
//        }
//
//        String renamingOpPattern = "^%([1-9][0-9]*) +as +\"([%a-z0-9A-Z-_. $&/]+)\"$";
//        pattern = Pattern.compile(renamingOpPattern);
//        Matcher renamingOpPatternMatcher = pattern.matcher(expr);
//        if(renamingOpPatternMatcher.matches()) {
//            return new RenameColumnExpression(Integer.parseInt(renamingOpPatternMatcher.group(1))-1, renamingOpPatternMatcher.group(2));
//        }
        throw new IllegalArgumentException("Cannot Resolve Expression : "+expr);
    }
}
