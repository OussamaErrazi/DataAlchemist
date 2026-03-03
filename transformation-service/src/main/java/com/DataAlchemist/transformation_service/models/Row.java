package com.DataAlchemist.transformation_service.models;

import com.DataAlchemist.transformation_service.context.SchemaContext;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Row {
    private List<Cell> cells = new ArrayList<>();

    public void addCell(Cell cell) {
        cells.add(cell);
    }

    public void construct(SchemaContext schemaCxt, Map<String, String> payload) {
        for(int i=0; i<payload.size(); i++) {
            addCell(Cell.builder()
                    .columnName(schemaCxt.getColumns().get(i))
                    .columnType(schemaCxt.getTypes().get(i))
                    .value(payload.get(schemaCxt.getColumns().get(i)))
                    .build()
                    );
        }

    }
}
