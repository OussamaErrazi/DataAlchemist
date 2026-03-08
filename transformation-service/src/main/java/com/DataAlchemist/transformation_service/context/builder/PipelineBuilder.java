package com.DataAlchemist.transformation_service.context.builder;

import com.DataAlchemist.transformation_service.context.PipelineContext;
import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.parser.Lexer;
import com.DataAlchemist.transformation_service.context.parser.Parser;
import com.DataAlchemist.transformation_service.context.parser.Token;
import com.DataAlchemist.transformation_service.context.pipe.AggregatePipe;
import com.DataAlchemist.transformation_service.context.pipe.FilterPipe;
import com.DataAlchemist.transformation_service.context.pipe.TransformationPipe;

import java.util.ArrayList;
import java.util.List;

public class PipelineBuilder {

    public static PipelineContext build(List<String> regexPipeline) {
        PipelineContext pipelineCxt = new PipelineContext();
        for(String expression : regexPipeline) {
            List<String> parts = splitByComma(expression);
            String first = parts.getFirst();
            if (first.toUpperCase().startsWith("FILTER(")) {
                if(parts.size()!=1) {
                    throw new IllegalArgumentException("Filter stage must contain exactly one condition. "+"Found " + parts.size() + " expressions. "+"Combine conditions using AND or OR operators instead.");
                }
                String stringExpression = first.substring(7, first.length()-1);
                pipelineCxt.addPipe(new FilterPipe(toColumnExpression(stringExpression)));
            } else if (first.toUpperCase().startsWith("GROUP(")) {
                if(parts.size()!=1) throw new IllegalArgumentException(); //todo exception message
                String stringExpression = first.substring(6, first.length()-1);
                List<String> arguments = splitByComma(stringExpression);
                pipelineCxt.setAggregating(true);
                pipelineCxt.addPipe(new AggregatePipe(arguments.stream().map(PipelineBuilder::toColumnExpression).toList()));
            } else {
                List<ColumnExpression> expressionList = new ArrayList<>();
                for (String stringExpression : parts) {

                    expressionList.add(toColumnExpression(stringExpression));
                }

                pipelineCxt.addPipe(new TransformationPipe(expressionList));
            }
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

    private static ColumnExpression toColumnExpression(String stringExpression) {
        List<Token> tokens = new Lexer(stringExpression).tokenize();
        return new Parser(tokens).parseColumnExpression();
    }
}
