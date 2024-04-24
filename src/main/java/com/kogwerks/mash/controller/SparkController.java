package com.kogwerks.mash.controller;

import com.kogwerks.mash.dto.ColumnStatisticDto;
import com.kogwerks.mash.dto.DistributionDto;
import com.kogwerks.mash.dto.TableSchemaDto;
import com.kogwerks.mash.service.SparkService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/profile")
@AllArgsConstructor
@Slf4j
@CrossOrigin("*")
public class SparkController {

    private final SparkService sparkService;

    @GetMapping("/{tableName}/schema")
    public TableSchemaDto getSchema(@PathVariable("tableName") String tableName) {
        return sparkService.getSchema(tableName);
    }

    @GetMapping("/{tableName}/duplicate/count")
    public Long getDuplicateRows(@PathVariable("tableName") String tableName) {
        return sparkService.getDuplicateRows(tableName);
    }

    @GetMapping("/{tableName}/distribution")
    public DistributionDto getDistribution(@PathVariable("tableName") String tableName) {
        return sparkService.getDistribution(tableName);
    }

    @GetMapping("/{tableName}/{columnName}/outliers")
    public Long getOutliers(@PathVariable("tableName") String tableName,
        @PathVariable("columnName") String columnName) {
        return sparkService.getOutliers(tableName, columnName);
    }

    @GetMapping("/{tableName}/{columnName}/describe")
    public ColumnStatisticDto describeColumn(@PathVariable("tableName") String tableName,
        @PathVariable("columnName") String columnName) {
        return sparkService.describeColumn(tableName, columnName);
    }

}
