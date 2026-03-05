package com.DataAlchemist.transformation_service.context.parser;

public enum TokenType {
    COLUMN_REF,
    INTEGER, DOUBLE, STRING, DATE, BOOLEAN, // current defined data types number can be integer or double
    IDENTIFIER, // reserved names like functions (ex if( condition, return, else return) filter(expression, condition) ...)
    PLUS, MINUS, MULTIPLY, DIVIDE,
    GT, GE, LT, LE, EQ, NEQ,
    MATCH,
    AND, OR,
    L_PARENTHESIS, R_PARENTHESIS,
    COMMA,
    AS, IS, //as : for column renaming, is : for changing type
    NULL,
    EOE //end of expression
}
