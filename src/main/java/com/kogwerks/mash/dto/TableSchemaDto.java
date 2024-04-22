package com.kogwerks.mash.dto;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableSchemaDto {

    private List<TableFieldDto> fields;
    private Long rows;
    private String size;
}
