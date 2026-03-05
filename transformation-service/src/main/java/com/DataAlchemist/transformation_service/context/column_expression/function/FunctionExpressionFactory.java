package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;

import java.util.List;

public class FunctionExpressionFactory {

    public ColumnExpression create(String functionName, List<ColumnExpression> inputs) {
        switch (functionName.toLowerCase()) {
            case "date" -> {
                if(inputs.size()!=3) throw new IllegalArgumentException("date function accepts 3 arguments but found "+inputs.size());
                return new DateFunctionExpression(inputs.get(0), inputs.get(1), inputs.get(2));
            }
            case "default" -> {
                if(inputs.size()!=2) throw new IllegalArgumentException("default function accepts 2 arguments but found "+inputs.size());
                return new DefaultFunctionExpression(inputs.getFirst(), inputs.getLast());
            }
            default -> throw new IllegalArgumentException("Unknown function name "+functionName);
        }
    }
}
