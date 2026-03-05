package com.DataAlchemist.transformation_service.context.parser;

import com.DataAlchemist.transformation_service.context.column_expression.*;
import com.DataAlchemist.transformation_service.context.column_expression.arithmetic.ArithmeticExpression;
import com.DataAlchemist.transformation_service.context.column_expression.comparison.ComparisonExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.FunctionExpressionFactory;
import com.DataAlchemist.transformation_service.context.column_expression.literal.LiteralExpression;
import com.DataAlchemist.transformation_service.models.enums.ColumnType;

import java.util.ArrayList;
import java.util.List;

public class Parser {
    private final List<Token> tokens;
    private int position = 0;

    public Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    public ColumnExpression parseColumnExpression() {
        ColumnExpression expr = parseBinaryOp();

        if(current().getType() == TokenType.AS) {
            consume();
            String alias = expect(TokenType.STRING).getValue();
            expr = new RenameColumnExpression(expr, alias);
        }

        if(current().getType() == TokenType.IS) {
            consume();
            String type = expect(TokenType.IDENTIFIER).getValue();
            expr = new TypeCastingExpression(expr, ColumnType.valueOf(type.toUpperCase()));
        }

        if(current().getType() != TokenType.EOE) {
            throw new IllegalArgumentException("Unexpected token at end: "+ current().getValue());
        }

        return expr;
    }

    private ColumnExpression parseBinaryOp() {
        return parseOrOp();
    }

    private ColumnExpression parseOrOp() {
        ColumnExpression left = parseAndOp();
        while(current().getType() == TokenType.OR) {
            consume();
            ColumnExpression right = parseAndOp();
            //todo implement logicOpExpression
            left = new LogicalOpExpression("or", left, right);
        }
        return left;
    }

    private ColumnExpression parseAndOp() {
        ColumnExpression left = parseComparison();
        while(current().getType() == TokenType.AND) {
            consume();
            ColumnExpression right = parseComparison();
            //todo implement logicOpExpression
            left = new LogicalOpExpression("and", left, right);
        }
        return left;
    }

    private ColumnExpression parseComparison() {
        ColumnExpression left = parseAddSub();
        while(isComparisonOp(current())) {
            String op = consume().getValue();
            ColumnExpression right = parseAddSub();
            //TODO implement the comparison expression class
            left = new ComparisonExpression(op, left, right);
        }
        return left;
    }

    private ColumnExpression parseAddSub() {
        ColumnExpression left = parseMulDiv();

        while(current().getType() == TokenType.PLUS || current().getType() == TokenType.MINUS) {
            String op = consume().getValue();
            ColumnExpression right = parseMulDiv();
            left = new ArithmeticExpression(op.charAt(0), left, right);
        }

        return left;
    }

    private ColumnExpression parseMulDiv() {
        ColumnExpression left = parseTerm();

        while(current().getType() == TokenType.MULTIPLY || current().getType() == TokenType.DIVIDE) {
            String op = consume().getValue();
            ColumnExpression right = parseTerm();
            left = new ArithmeticExpression(op.charAt(0), left, right);
        }

        return left;
    }

    private ColumnExpression parseTerm() {
        Token t = current();
        switch (t.getType()) {
            case TokenType.COLUMN_REF -> {consume(); return new ColumnRefExpression(Integer.parseInt(t.getValue())-1);}
            case TokenType.INTEGER -> {consume(); return new LiteralExpression(Integer.parseInt(t.getValue()));}
            case TokenType.DOUBLE -> {consume(); return new LiteralExpression(Double.parseDouble(t.getValue()));}
            case TokenType.BOOLEAN -> {consume(); return new LiteralExpression(t.getValue().equals("true"));}
            case TokenType.STRING -> {consume(); return new LiteralExpression(t.getValue());}
            case TokenType.NULL -> {consume(); return new NullExpression();}
            case TokenType.IDENTIFIER -> {return parseFunctionCall();}
            case TokenType.L_PARENTHESIS -> {
                consume();
                ColumnExpression innerExpression = parseAddSub();
                expect(TokenType.R_PARENTHESIS);
                return innerExpression;
            }
            default -> throw new IllegalArgumentException("Unexpected token: "+t.getValue()+" of type "+t.getType());
        }
    }

    //todo implement the function expression by creating function expression class
    private ColumnExpression parseFunctionCall(){
        String functionName = consume().getValue();
        List<ColumnExpression> inputs = new ArrayList<>();
        expect(TokenType.L_PARENTHESIS);
        while(true) {
            if(current().getType() == TokenType.EOE) throw new IllegalArgumentException("Missing closing parenthesis.");
            if(current().getType() == TokenType.R_PARENTHESIS) break;

            if(current().getType() == TokenType.COMMA){
                inputs.add(null);
                consume();
                continue;
            }
            inputs.add(parseBinaryOp());
            if(current().getType() == TokenType.COMMA){
                consume();
                if(current().getType() == TokenType.R_PARENTHESIS){
                    inputs.add(null);
                    break;
                }
            }
        }
        expect(TokenType.R_PARENTHESIS);
        return new FunctionExpressionFactory().create(functionName, inputs);
    }

    private boolean isComparisonOp(Token t) {
        return switch (t.getType()) {
            case TokenType.GT, TokenType.GE,
                 TokenType.LT, TokenType.LE, TokenType.EQ, TokenType.NEQ,
                 TokenType.MATCH
                    -> true;
            default -> false;
        };
    }

    private Token current() {
        return tokens.get(position);
    }

    private Token consume() {
        return tokens.get(position++);
    }

    private Token expect(TokenType type) {
        if(current().getType() != type) {
            throw new IllegalArgumentException("Expect "+type+" but found "+current().getType()+ " ( "+current().getValue()+" )");
        }
        return consume();
    }
}
