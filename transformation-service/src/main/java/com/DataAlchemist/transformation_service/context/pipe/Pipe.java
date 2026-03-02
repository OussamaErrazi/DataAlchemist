package com.DataAlchemist.transformation_service.context.pipe;

import com.DataAlchemist.transformation_service.models.Row;

public interface Pipe {
    Row process(Row row);
}
