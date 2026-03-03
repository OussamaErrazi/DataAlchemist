package com.DataAlchemist.transformation_service.context.column_expression;

import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

public class ArithmeticOpExpression implements ColumnExpression{
    private final char op;
    private final int leftOperand;
    private final int rightOperand;

    public ArithmeticOpExpression(char op, int leftOperand, int rightOperand) {
        this.op = op;
        this.leftOperand = leftOperand;
        this.rightOperand = rightOperand;
    }

    //TODO complete this evaluate method
    @Override
    public Cell evaluate(Row row) {
        Cell leftCell = row.getCells().get(leftOperand);
        Cell rightCell = row.getCells().get(rightOperand);
        switch (this.op) {
            case '+' -> {
                return add(leftCell, rightCell);
            }
            case '-' -> {
                return subtract(leftCell, rightCell);
            }
            case '*' -> {
                return multiply(leftCell, rightCell);
            }
            case '/' -> {
                return divide(leftCell, rightCell);
            }
            default -> throw new IllegalArgumentException("Unsupported Operation "+op);
        }
    }

    //available types currently Integer, Double, Date, String
    private Cell add(Cell left, Cell right) {
        Cell cell = new Cell();
        cell.setColumnName(left.getColumnName()+ " + "+right.getColumnName());
        if (left.getColumnType() == ColumnType.STRING || right.getColumnType() == ColumnType.STRING) {
            cell.setColumnType(ColumnType.STRING);
            cell.setValue(left.getValue().toString()+right.getValue().toString());
        } else if (left.isNumeric()) {
            if (right.isNumeric()) {
                cell.setColumnType(left.getColumnType() == ColumnType.DOUBLE || right.getColumnType() == ColumnType.DOUBLE ?  ColumnType.DOUBLE : ColumnType.INTEGER);
                if(cell.getColumnType() == ColumnType.INTEGER) cell.setValue(Integer.parseInt(left.getValue().toString()) + Integer.parseInt(right.getValue().toString()));
                else cell.setValue(Double.parseDouble(left.getValue().toString()) + Double.parseDouble(right.getValue().toString()));
            } else throw new IllegalArgumentException("Unsupported Addition Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        } else throw new IllegalArgumentException("Unsupported Addition Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        return cell;
    }

    //available types currently Integer, Double, Date, String
    private Cell subtract(Cell left, Cell right) {
        Cell cell = new Cell();
        cell.setColumnName(left.getColumnName()+ " - "+right.getColumnName());
        if (left.isNumeric()) {
            if (right.isNumeric()) {
                cell.setColumnType(left.getColumnType() == ColumnType.DOUBLE || right.getColumnType() == ColumnType.DOUBLE ? ColumnType.DOUBLE : ColumnType.INTEGER);
                if(cell.getColumnType() == ColumnType.INTEGER) cell.setValue(Integer.parseInt(left.getValue().toString()) - Integer.parseInt(right.getValue().toString()));
                else cell.setValue(Double.parseDouble(left.getValue().toString()) - Double.parseDouble(right.getValue().toString()));
            } else throw new IllegalArgumentException("Unsupported Subtraction Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        } else throw new IllegalArgumentException("Unsupported Subtraction Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        return cell;
    }

    //available types currently Integer, Double, Date, String
    private Cell divide(Cell left, Cell right) {
        Cell cell = new Cell();
        cell.setColumnName(left.getColumnName()+ " / "+right.getColumnName());
        if (left.isNumeric()) {
            if (right.isNumeric()) {
                cell.setColumnType(ColumnType.DOUBLE);
                cell.setValue(Double.parseDouble(left.getValue().toString()) / Double.parseDouble(right.getValue().toString()));
            } else throw new IllegalArgumentException("Unsupported Division Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        } else throw new IllegalArgumentException("Unsupported Division Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        return cell;
    }

    //available types currently Integer, Double, Date, String
    private Cell multiply(Cell left, Cell right) {
        Cell cell = new Cell();
        cell.setColumnName(left.getColumnName()+ " * "+right.getColumnName());
        if(left.getColumnType() == ColumnType.STRING) {
            if (right.getColumnType() == ColumnType.INTEGER) {
                cell.setColumnType(ColumnType.STRING);
                cell.setValue(left.getValue().toString().repeat(Integer.parseInt(right.getValue().toString())));
            } else throw new IllegalArgumentException("Unsupported Multiplication Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        } else if (right.getColumnType() == ColumnType.STRING) {
            if(left.getColumnType() == ColumnType.INTEGER) {
                cell.setColumnType(ColumnType.STRING);
                cell.setValue(right.getValue().toString().repeat(Integer.parseInt(left.getValue().toString())));
            } else throw new IllegalArgumentException("Unsupported Multiplication Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        } else if (left.isNumeric()) {
            if (right.isNumeric()) {
                cell.setColumnType(left.getColumnType() == ColumnType.DOUBLE || right.getColumnType() == ColumnType.DOUBLE ?  ColumnType.DOUBLE : ColumnType.INTEGER);
                if (cell.getColumnType() == ColumnType.DOUBLE) cell.setValue(Double.parseDouble(left.getValue().toString()) * Double.parseDouble(right.getValue().toString()));
                else cell.setValue(Integer.parseInt(left.getValue().toString()) * Integer.parseInt(right.getValue().toString()));
            } else throw new IllegalArgumentException("Unsupported Multiplication Operation between : " + left.getColumnType() + " & "+right.getColumnType());
        }
        return cell;
    }
}
