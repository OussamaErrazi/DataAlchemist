package com.DataAlchemist.transformation_service.context.pipe;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateExpression;
import com.DataAlchemist.transformation_service.context.column_expression.function.aggregate.AggregateNode;
import com.DataAlchemist.transformation_service.models.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class AggregatePipe implements Pipe{
    private final AggregateNode root = new AggregateNode();
    private final List<ColumnExpression> expressionList;
    private final List<AggregateExpression> aggregateExpressionList;

    public AggregatePipe(List<ColumnExpression> expressions) {
        int i=0;
        expressionList = new ArrayList<>();
        aggregateExpressionList = new ArrayList<>();
        while(i<expressions.size()) {
            ColumnExpression expression = expressions.get(i);
            if(expression instanceof AggregateExpression) break;
            expressionList.add(expression);
            i++;
        }
        while(i<expressions.size()) {
            ColumnExpression aggregateExpression = expressions.get(i);
            if(aggregateExpression instanceof AggregateExpression a) {
                aggregateExpressionList.add(a);
            } else {
                throw new IllegalArgumentException(); //todo add message
            }
            i++;
        }
        if(aggregateExpressionList.isEmpty()) throw new IllegalArgumentException(); //todo exception message
    }

    @Override
    public Row process(Row row) {
        root.construct(row, expressionList, aggregateExpressionList);
        return null;
    }

    public void produce(Consumer<Row> consumer) {
        root.produce(new Row(), consumer);
    }
}
