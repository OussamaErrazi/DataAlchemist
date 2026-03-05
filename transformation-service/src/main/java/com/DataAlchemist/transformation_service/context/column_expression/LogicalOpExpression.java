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
        Boolean l = (Boolean) lcell.getValue();
        Boolean r = (Boolean) rcell.getValue();

        if(op.equals("or")) {
            if(Boolean.TRUE.equals(l) || Boolean.TRUE.equals(r))
                return Cell.builder()
                        .value(true)
                        .columnType(ColumnType.BOOLEAN)
                        .build();
            if(l == null || r == null)
                return Cell.builder()
                        .value(null)
                        .columnType(ColumnType.BOOLEAN)
                        .build();
            return Cell.builder()
                    .value(false)
                    .columnType(ColumnType.BOOLEAN)
                    .build();
        }

        if(op.equals("and")) {
            if(Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r))
                return Cell.builder()
                        .value(false)
                        .columnType(ColumnType.BOOLEAN)
                        .build();
            if(l == null || r == null)
                return Cell.builder()
                        .value(null)
                        .columnType(ColumnType.BOOLEAN)
                        .build();
            return Cell.builder()
                    .value(true)
                    .columnType(ColumnType.BOOLEAN)
                    .build();
        }


        throw new IllegalArgumentException("Unsupported logical operation "+op);
    }
}
