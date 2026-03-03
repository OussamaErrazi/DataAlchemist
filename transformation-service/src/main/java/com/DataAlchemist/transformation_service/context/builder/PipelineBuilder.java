package com.DataAlchemist.transformation_service.context.builder;

import com.DataAlchemist.transformation_service.context.PipelineContext;
import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpressionResolver;
import com.DataAlchemist.transformation_service.context.pipe.TransformationPipe;

import java.util.List;

public class PipelineBuilder {

    public static PipelineContext build(List<String> regexPipeline) {
        PipelineContext pipelineCxt = new PipelineContext();
        for(String expression : regexPipeline) {
            String[] tokens = tokenizer(expression);
            TransformationPipe pipe = new TransformationPipe();
            for(String token : tokens) {
                pipe.addColumnExpression(ColumnExpressionResolver.resolve(token));
            }

            pipelineCxt.addPipe(pipe);
        }

        return pipelineCxt;
    }

    private static String[] tokenizer(String expression) {
        return expression.split(",");
    }
}
