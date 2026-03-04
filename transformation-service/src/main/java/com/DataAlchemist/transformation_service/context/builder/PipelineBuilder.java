package com.DataAlchemist.transformation_service.context.builder;

import com.DataAlchemist.transformation_service.context.PipelineContext;
import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.parser.Lexer;
import com.DataAlchemist.transformation_service.context.parser.Parser;
import com.DataAlchemist.transformation_service.context.parser.Token;
import com.DataAlchemist.transformation_service.context.pipe.TransformationPipe;

import java.util.ArrayList;
import java.util.List;

public class PipelineBuilder {

    public static PipelineContext build(List<String> regexPipeline) {
        PipelineContext pipelineCxt = new PipelineContext();
        for(String expression : regexPipeline) {
            List<String> parts = splitByComma(expression);
            List<ColumnExpression> expressionList = new ArrayList<>();
            for(String colExpression : parts) {
                List<Token> tokens = new Lexer(colExpression).tokenize();
                ColumnExpression expr = new Parser(tokens).parseColumnExpression();
                expressionList.add(expr);
            }

            pipelineCxt.addPipe(new TransformationPipe(expressionList));
        }

        return pipelineCxt;
    }

    private static List<String> splitByComma(String expression) {
        List<String> parts = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int parenthesis_depth =0;
        boolean insideString = false;
        for(char c : expression.toCharArray()) {
            if(c == '\'') {
                insideString=!insideString;
            }
            if(!insideString && c == '(') parenthesis_depth++;
            else if(!insideString && c == ')') parenthesis_depth--;
            else if(!insideString && parenthesis_depth==0 && c==',') {
                parts.add(sb.toString().trim());
                sb = new StringBuilder();
                continue;
            }
            sb.append(c);
        }
        if(!sb.isEmpty()) {
            parts.add(sb.toString().trim());
        }
        return parts;
    }
}
