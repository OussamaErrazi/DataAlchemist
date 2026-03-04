package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class TypeCastingExpression implements ColumnExpression{
    private final ColumnExpression column;
    private final ColumnType newType;

    public TypeCastingExpression(ColumnExpression column, ColumnType newType) {
        this.column = column;
        this.newType = newType;
    }

    @Override
    public Cell evaluate(Row row) {
        Cell cell = column.evaluate(row);
        switch (newType) {
            case ColumnType.DOUBLE -> cell.setValue(Double.parseDouble(cell.getValue().toString()));
            case ColumnType.INTEGER -> cell.setValue((int) Double.parseDouble(cell.getValue().toString()));
            case ColumnType.STRING -> cell.setValue(cell.getValue().toString());
            default -> throw new IllegalArgumentException("Unsupported casting operation: casting value "+cell.getValue()+" to type "+newType);
        }
        cell.setColumnType(newType);
        return cell;
    }
}
