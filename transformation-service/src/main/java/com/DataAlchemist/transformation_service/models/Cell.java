package com.DataAlchemist.transformation_service.models;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Cell {

    private Object value;
    private String columnName;
    private String columnType;
}
