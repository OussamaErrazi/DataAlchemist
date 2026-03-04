package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class LogicalOpExpression implements ColumnExpression{
    private final String op;
    private final ColumnExpression left;
    private final ColumnExpression right;

    public LogicalOpExpression(String op, ColumnExpression left, ColumnExpression right) {
        this.op = op;
        this.left = left;
        this.right = right;
    }


    @Override
    public Cell evaluate(Row row) {
        Cell lcell = left.evaluate(row);
        Cell rcell = right.evaluate(row);
        if(lcell.getColumnType() != ColumnType.BOOLEAN && rcell.getColumnType() != ColumnType.BOOLEAN) {
            throw new IllegalArgumentException("Unsupported operation '"+op+"' between types "+lcell.getColumnType()+" and "+rcell.getColumnType());
        }
        Cell result = Cell.builder()
                .columnType(ColumnType.BOOLEAN)
                .build();

        switch (op) {
            case "or" -> result.setValue(Boolean.parseBoolean(lcell.getValue().toString()) || Boolean.parseBoolean(rcell.getValue().toString()));
            case "and" -> result.setValue(Boolean.parseBoolean(lcell.getValue().toString()) && Boolean.parseBoolean(rcell.getValue().toString()));
            default -> throw new IllegalArgumentException("Unsupported logical operation "+op);
        };

        return result;
    }
}
