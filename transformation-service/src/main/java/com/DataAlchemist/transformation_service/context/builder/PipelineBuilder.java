package com.DataAlchemist.transformation_service.context.builder;

import com.DataAlchemist.transformation_service.context.PipelineContext;
import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpressionResolver;
import com.DataAlchemist.transformation_service.context.pipe.TransformationPipe;

import java.util.ArrayList;
import java.util.List;

public class PipelineBuilder {

    public static PipelineContext build(List<String> regexPipeline) {
        PipelineContext pipelineCxt = new PipelineContext();
        for(String expression : regexPipeline) {
            List<String> tokens = splitByComma(expression);
            TransformationPipe pipe = new TransformationPipe();
            for(String token : tokens) {
                pipe.addColumnExpression(ColumnExpressionResolver.resolve(token));
            }

            pipelineCxt.addPipe(pipe);
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
