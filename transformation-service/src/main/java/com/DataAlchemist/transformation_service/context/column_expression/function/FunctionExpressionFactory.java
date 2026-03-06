package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.string_function.*;

import java.util.List;

public class FunctionExpressionFactory {

    public ColumnExpression create(String functionName, List<ColumnExpression> inputs) {
        switch (functionName.toLowerCase()) {
            case "date" -> {
                if(inputs.size()!=3) throwError("date", 3, inputs.size());
                return new DateFunctionExpression(inputs.get(0), inputs.get(1), inputs.get(2));
            }
            case "default" -> {
                if(inputs.size()!=2) throwError("default", 2, inputs.size());
                return new DefaultFunctionExpression(inputs.getFirst(), inputs.getLast());
            }
            case "if" -> {
                if(inputs.size()!=3) throwError("if", 3, inputs.size());
                return new IfFunctionExpression(inputs.get(0), inputs.get(1), inputs.get(2));
            }
            case "not" -> {
                if(inputs.size()!=1) throwError("not", 1, inputs.size());
                return new NotFunctionExpression(inputs.getFirst());
            }
            case "trim" -> {
                if(inputs.size()!=1) throwError("trim", 1 , inputs.size());
                return new TrimFunctionExpression(inputs.getFirst());
            }
            case "upper" -> {
                if(inputs.size()!=1) throwError("upper", 1 , inputs.size());
                return new UpperFunctionExpression(inputs.getFirst());
            }
            case "lower" -> {
                if(inputs.size()!=1) throwError("lower", 1 , inputs.size());
                return new LowerFunctionExpression(inputs.getFirst());
            }
            case "length" -> {
                if(inputs.size()!=1) throwError("length", 1 , inputs.size());
                return new LengthFunctionExpression(inputs.getFirst());
            }
            case "contains" -> {
                if(inputs.size()!=2) throwError("contains", 2, inputs.size());
                return new ContainsFunctionExpression(inputs.getFirst(), inputs.getLast());
            }
            case "substring" -> {
                if(inputs.size()!=3) throwError("substring", 3, inputs.size());
                return new SubstringFunctionExpression(inputs.getFirst(), inputs.get(1), inputs.getLast());
            }
            case "index_of" -> {
                if(inputs.size()!=2) throwError("index_of", 2, inputs.size());
                return new IndexOfFunctionExpression(inputs.getFirst(), inputs.getLast());
            }
            case "starts_with" -> {
                if(inputs.size()!=2) throwError("starts_with", 2, inputs.size());
                return new StartsWithFunctionExpression(inputs.getFirst(), inputs.getLast());
            }
            case "ends_with" -> {
                if(inputs.size()!=2) throwError("ends_with", 2, inputs.size());
                return new EndsWithFunctionExpression(inputs.getFirst(), inputs.getLast());
            }
            case "replace" -> {
                if(inputs.size()!=3) throwError("replace", 3, inputs.size());
                return new ReplaceFunctionExpression(inputs.getFirst(), inputs.get(1), inputs.getLast());
            }
            default -> throw new IllegalArgumentException("Unknown function name "+functionName);
        }
    }

    private void throwError(String functionName, int expectedSize, int foundSize) throws IllegalArgumentException{
        throw new IllegalArgumentException("'"+functionName+", function accepts "+expectedSize+" arguments but found "+foundSize);
    }
}
