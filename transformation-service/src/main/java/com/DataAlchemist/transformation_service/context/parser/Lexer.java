package com.DataAlchemist.transformation_service.context.parser;


import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Lexer {

    private final String expression;
    private int position = 0;

    public Lexer(String expression) {
        this.expression = expression;
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();

        while (position < expression.length()) {
            skipWhiteSpace();
            if(position >= expression.length()) break;
            tokens.add(readNext());
        }
        tokens.add(new Token("", TokenType.EOE));
        return tokens;
    }

    private Token readNext() {
        char c = expression.charAt(position);

        if (c== '%') return readColumnRef();
        if (Character.isDigit(c)) return readNumber();
        if (c == '"') return readString();
        if(Character.isLetter(c)) return readIdentifierOrKeyword();
        if(c == '(') {position++; return new Token("(", TokenType.L_PARENTHESIS);}
        if(c == ')') {position++; return new Token(")", TokenType.R_PARENTHESIS);}
        if(c == ',') {position++; return new Token(",", TokenType.COMMA);}
        if(c == '+') {position++; return new Token("+", TokenType.PLUS);}
        if(c == '-') {position++; return new Token("-", TokenType.MINUS);}
        if(c == '*') {position++; return new Token("*", TokenType.MULTIPLY);}
        if(c == '/') {position++; return new Token("/", TokenType.DIVIDE);}
        if(c=='<') {
            if(peek() == '=') {position+=2;return new Token("<=", TokenType.LE);}
            else {position++;return new Token("<", TokenType.LT);}
        }
        if(c=='>') {
            if(peek() == '=') {position+=2;return new Token(">=", TokenType.GE);}
            else {position++;return new Token(">", TokenType.GT);}
        }
        if(c == '!' && peek()=='=') {position+=2; return new Token("!=", TokenType.NEQ);}
        if(c == '=' && peek()=='=') {position+=2; return new Token("==", TokenType.EQ);}


        throw new IllegalArgumentException("Unknown character : "+ c + " at position "+position+" in expression "+expression);
    }

    private Token readColumnRef() {
        position++;
        StringBuilder sb = new StringBuilder();
        while(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(sb.isEmpty()) throw new IllegalArgumentException("expected column ref number at position "+position +" in expression "+expression);
        return new Token(sb.toString(), TokenType.COLUMN_REF);
    }

    private Token readNumber() {
        StringBuilder sb = new StringBuilder();

        while(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(position<expression.length() && expression.charAt(position) == '.') {
            sb.append('.');
            position++;
        }
        while(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        String number = sb.toString();
        try{
            Integer.parseInt(number);
            return new Token(number, TokenType.INTEGER);
        } catch (Exception ignored) {
            return new Token(number, TokenType.DOUBLE);
        }
    }

    private Token readIdentifierOrKeyword() {
        StringBuilder sb = new StringBuilder();
        while(position<expression.length() && (Character.isLetterOrDigit(expression.charAt(position)) || expression.charAt(position) == '_')) {
            sb.append(expression.charAt(position++));
        }
        String word = sb.toString();
        return switch (word.toLowerCase()) {
            case "and" -> new Token("and", TokenType.AND);
            case "or" -> new Token("or", TokenType.OR);
            case "as" -> new Token("as", TokenType.AS);
            case "is" -> new Token("is", TokenType.IS);
            case "true" -> new Token("true", TokenType.BOOLEAN);
            case "false" -> new Token("false", TokenType.BOOLEAN);
            case "date" -> readDate();
            default -> new Token(word, TokenType.IDENTIFIER);
        };
    }

    private Token readDate() {
        StringBuilder sb = new StringBuilder();
        if(position<expression.length() && expression.charAt(position) == '(') {
            sb.append('(');
            position++;
        }
        skipWhiteSpace();

        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }

        skipWhiteSpace();
        if(position<expression.length() && expression.charAt(position) == ',') {
            sb.append(',');position++;
        }
        skipWhiteSpace();

        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }

        skipWhiteSpace();
        if(position<expression.length() && expression.charAt(position) == ',') {
            sb.append(',');position++;
        }
        skipWhiteSpace();

        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }
        if(position<expression.length() && Character.isDigit(expression.charAt(position))) {
            sb.append(expression.charAt(position++));
        }

        skipWhiteSpace();
        if(position<expression.length() && expression.charAt(position) == ')') {
            sb.append(')');
            position++;
        }
        String word = sb.toString();
        String datePattern = "^\\(([0-9]{0,2}),([0-9]{0,2}),([0-9]{0,4})\\)$";
        Pattern pattern = Pattern.compile(datePattern);
        Matcher dateMatcher = pattern.matcher(word);
        if(dateMatcher.matches()) {
            String day = dateMatcher.group(1);
            String month = dateMatcher.group(2);
            String year = dateMatcher.group(3);
            if(!day.isEmpty() || !month.isEmpty() || !year.isEmpty()) {
                if(!day.isEmpty()) {
                    int dayN = Integer.parseInt(day);
                    if(dayN<1 || dayN>31) {
                        throw new IllegalArgumentException("Invalid date date"+word+" in expression "+expression);
                    }
                }
                if(!month.isEmpty()) {
                    int monthN = Integer.parseInt(month);
                    if(monthN<1 || monthN>12) {
                        throw new IllegalArgumentException("Invalid date date"+word+" in expression "+expression);
                    }
                }

                if(!year.isEmpty()) {
                    int yearN = Integer.parseInt(month);
                    if(yearN<0 || yearN>9999) {
                        throw new IllegalArgumentException("Invalid date date"+word+" in expression "+expression);
                    }
                }
                return new Token("date"+word, TokenType.DATE);
            }
        }
        throw new IllegalArgumentException("Invalid date date"+word+" in expression "+expression);
    }

    private Token readString() {
        StringBuilder sb = new StringBuilder();
        position++;
        while(position<expression.length() && expression.charAt(position) != '"') {
            sb.append(expression.charAt(position++));
        }
        position++;
        return new Token(sb.toString(), TokenType.STRING);
    }

    private void skipWhiteSpace() {
        while(position < expression.length() && Character.isWhitespace(expression.charAt(position))) position++;
    }

    private char peek() {
        return position+1<expression.length() ? expression.charAt(position+1) : '\0';
    }
}
