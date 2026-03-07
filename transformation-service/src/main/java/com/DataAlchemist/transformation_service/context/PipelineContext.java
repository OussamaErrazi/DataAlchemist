package com.DataAlchemist.transformation_service.context;

import com.DataAlchemist.transformation_service.context.pipe.AggregatePipe;
import com.DataAlchemist.transformation_service.context.pipe.FilterPipe;
import com.DataAlchemist.transformation_service.context.pipe.Pipe;
import com.DataAlchemist.transformation_service.models.Row;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PipelineContext {

    private final List<Pipe> pipes = new ArrayList<>();
    private boolean aggregating = false;
    private int aggregateI = 0;

    public void addPipe(Pipe pipe) {
        this.pipes.add(pipe);
    }

    public Row process(Row row) {
        int index=0;
        for(Pipe pipe : pipes) {
            if(row == null) return null;
            if(pipe instanceof AggregatePipe a) {
                a.process(row);
                aggregating = true; aggregateI = index;return null;
            }
            row = pipe.process(row);
            index++;
        }
        return row;
    }

    public void flush(Consumer<Row> consumer) {
        if(!aggregating) return;
        if(aggregateI < pipes.size()){
            AggregatePipe aggregatePipe = (AggregatePipe) pipes.get(aggregateI);
            aggregatePipe.produce(row-> {
                Row result = continueFrom(row, aggregateI+1);
                if(result != null) consumer.accept(result);
            });
        }
    }

    private Row continueFrom(Row row, int index) {
        for(int i=index; i< pipes.size(); i++) {
            if(row == null) return null;
            row = pipes.get(i).process(row);
        }
        return row;
    }
}
