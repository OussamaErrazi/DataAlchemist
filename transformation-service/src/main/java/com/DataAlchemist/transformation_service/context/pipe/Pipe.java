package com.DataAlchemist.transformation_service.context.pipe;

import com.DataAlchemist.transformation_service.models.Row;

import java.util.Set;

public interface Pipe {
    Set<String> STAGE_KEYWORD_SET = Set.of("filter");

    Row process(Row row);
}
