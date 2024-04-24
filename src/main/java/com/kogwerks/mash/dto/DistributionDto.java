package com.kogwerks.mash.dto;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DistributionDto {

    private List<String>                  columns;
    private List<Map<String, String>> rows;

}
