package com.DataAlchemist.transformation_service.context.column_expression;

import java.util.List;

public class FunctionExpressionFactory {

    public ColumnExpression create(String functionName, List<ColumnExpression> inputs) {
        switch (functionName.toLowerCase()) {
            case "date" -> {
                //todo make date as identifier instead of a date token
                if(inputs.size()!=3) throw new IllegalArgumentException("date function accepts 3 arguments but found "+inputs.size());
                return new DateLiteralExpression(inputs.get(0), inputs.get(1), inputs.get(2));
            }
        }
        return null;
    }
}
