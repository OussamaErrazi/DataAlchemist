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
        for(int pipeI =0 ; pipeI < pipes.size(); pipeI++) {
            if(row == null) return null;
            Pipe pipe = pipes.get(pipeI);
            if(pipe instanceof AggregatePipe a) {
                a.process(row);
                aggregateI = pipeI; return null;
            }
            row = pipe.process(row);
        }
        return row;
    }

    public void flush(Consumer<Row> consumer) {
        if(!aggregating || aggregateI >= pipes.size()) return;
        Pipe p = pipes.get(aggregateI);
        if(p instanceof AggregatePipe aggregatePipe){
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
