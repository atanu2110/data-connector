package com.kogwerks.mash.dto;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TableProfileDto {

    private String            schema;
    private Map<String, Long> missingValue;
    private Map<String, Long> uniqueValue;
    private String outliers;
    private String stats;

}
