package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;

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
            default -> throw new IllegalArgumentException("Unknown function name "+functionName);
        }
    }

    private void throwError(String functionName, int expectedSize, int foundSize) throws IllegalArgumentException{
        throw new IllegalArgumentException("'"+functionName+", function accepts "+expectedSize+" arguments but found "+foundSize);
    }
}
