package com.kogwerks.mash.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableFieldDto {

    private String  name;
    private String  type;
    private boolean nullable;

    private String missingValuesCount;
    private String uniqueValuesCount;
}
