package com.DataAlchemist.transformation_service.context.column_expression.function.aggregate;

import com.DataAlchemist.transformation_service.context.column_expression.ColumnExpression;
import com.DataAlchemist.transformation_service.models.Cell;
import com.DataAlchemist.transformation_service.models.Row;

import java.util.*;
import java.util.function.Consumer;

public class AggregateNode{
    private final Map<Cell, AggregateNode> aggregateTree = new LinkedHashMap<>();
    private final List<AggregateExpression> aggregateExpressions = new ArrayList<>();

    public void construct(Row row, List<ColumnExpression> keys, List<AggregateExpression> aggregates) {
        if (keys.isEmpty()) {
            if(aggregateExpressions.isEmpty()) aggregateExpressions.addAll(aggregates.stream().map(AggregateExpression::copy).toList());
            aggregateExpressions.forEach(a -> a.addRow(row));
        } else {
            ColumnExpression first = keys.getFirst();
            Cell result = first.evaluate(row);
            if(!aggregateTree.containsKey(result)) {
                aggregateTree.put(result, new AggregateNode());
            }
            AggregateNode node = aggregateTree.get(result);
            node.construct(row, keys.subList(1, keys.size()), aggregates);
        }
    }

    public void produce(Row row, Consumer<Row> consumer) {
        if(aggregateTree.isEmpty()) {
            aggregateExpressions.forEach(a -> {
                row.addCell(a.get());
            });
            consumer.accept(row);
            return;
        }
        for(Map.Entry<Cell, AggregateNode> entry : aggregateTree.entrySet()) {
            Row rCopy = row.shallowCopy();
            rCopy.addCell(entry.getKey());
            entry.getValue().produce(rCopy, consumer);
        }
    }
}
