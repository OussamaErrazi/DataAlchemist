package com.DataAlchemist.transformation_service.context.parser;

import com.DataAlchemist.transformation_service.context.column_expression.*;

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
            String type = expect(TokenType.STRING).getValue();
            //todo use TypeCasterExpression class here after creating it
        }

        if(current().getType() != TokenType.EOE) {
            throw new IllegalArgumentException("Unexpected token at end: "+ current().getValue());
        }

        return expr;
    }

    private ColumnExpression parseBinaryOp() {
        return parseComparison();
    }

    private ColumnExpression parseComparison() {
        ColumnExpression left = parseAddSub();
        while(isComparisonOp(current())) {
            String op = consume().getValue();
            ColumnExpression right = parseAddSub();
            //TODO implement the comparison expression class
//            left = new ComparisonExpression(op, left, right);
        }
        return left;
    }

    private ColumnExpression parseAddSub() {
        ColumnExpression left = parseMulDiv();

        while(current().getType() == TokenType.PLUS || current().getType() == TokenType.MINUS) {
            String op = consume().getValue();
            ColumnExpression right = parseMulDiv();
            left = new ArithmeticOpExpression(op.charAt(0), left, right);
        }

        return left;
    }

    private ColumnExpression parseMulDiv() {
        ColumnExpression left = parseTerm();

        while(current().getType() == TokenType.MULTIPLY || current().getType() == TokenType.DIVIDE) {
            String op = consume().getValue();
            ColumnExpression right = parseTerm();
            left = new ArithmeticOpExpression(op.charAt(0), left, right);
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
            case TokenType.DATE -> {consume(); return new DateLiteralExpression(t.getValue());}
            //todo implement the function expression
//            case TokenType.IDENTIFIER -> {
//
////                return parseFunctionCall();
//            }
            case TokenType.L_PARENTHESIS -> {
                consume();
                ColumnExpression innerExpression = parseAddSub();
                expect(TokenType.R_PARENTHESIS);
                return innerExpression;
            }
            default -> throw new IllegalArgumentException("Unexpected token: "+t.getValue()+" of type "+t.getType());
        }
    }

    private boolean isComparisonOp(Token t) {
        return switch (t.getType()) {
            case TokenType.GT, TokenType.GE,
                 TokenType.LT, TokenType.LE, TokenType.EQ, TokenType.NEQ -> true;
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
