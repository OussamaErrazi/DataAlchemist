package com.DataAlchemist.transformation_service.context;

import com.DataAlchemist.transformation_service.context.pipe.Pipe;
import com.DataAlchemist.transformation_service.models.Row;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
public class PipelineContext {

    private final List<Pipe> pipes = new ArrayList<>();

    public void addPipe(Pipe pipe) {
        this.pipes.add(pipe);
    }

    public Row process(Row row) {
        for(Pipe pipe : pipes) {
            if(row == null) return null;
            row = pipe.process(row);
        }
        return row;
    }
}
