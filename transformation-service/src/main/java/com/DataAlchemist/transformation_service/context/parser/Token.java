package com.DataAlchemist.transformation_service.context.parser;

import lombok.Getter;

@Getter
public class Token {
    private final String value;
    private final TokenType type;

    public Token(String value, TokenType type) {
        this.value = value;
        this.type = type;
    }
}
