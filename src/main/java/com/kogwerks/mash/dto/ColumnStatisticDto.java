package com.kogwerks.mash.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ColumnStatisticDto {

    private String columnName;
    private String missingValuesCount;
    private String uniqueValuesCount;
    private String categoricalCount;
    private double dataQualityPercentage;

}
