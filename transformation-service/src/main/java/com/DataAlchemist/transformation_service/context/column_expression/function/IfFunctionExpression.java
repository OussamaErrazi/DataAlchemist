package com.DataAlchemist.transformation_service.context.column_expression.function;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class IfFunctionExpression implements ColumnExpression {
    private final ColumnExpression condition;
    private final ColumnExpression succeed;
    private final ColumnExpression fail;

    public IfFunctionExpression(ColumnExpression condition, ColumnExpression succeed, ColumnExpression fail) {
        this.condition = condition;
        this.succeed = succeed;
        this.fail = fail;
    }


    @Override
    public Cell evaluate(Row row) {
        Cell conditionResult = condition.evaluate(row);
        if(conditionResult.getValue() == null) throw new IllegalArgumentException("if condition evaluated to null");
        if(conditionResult.getColumnType()!= ColumnType.BOOLEAN) throw new IllegalArgumentException("Expected boolean value but found "+conditionResult.getColumnType());
        Cell succeeded = succeed.evaluate(row);
        Cell failed = fail.evaluate(row);
        if(succeeded.getColumnType() != null && failed.getColumnType() != null && succeeded.getColumnType() != failed.getColumnType()) throw new IllegalArgumentException("If branch type mismatch. then branch returns "+succeeded.getColumnType()+ " and else branch returns "+failed.getColumnType());
        return Boolean.TRUE.equals(conditionResult.getValue()) ? succeeded : failed;
    }
}
