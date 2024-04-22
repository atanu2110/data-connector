package com.kogwerks.mash.controller;

import com.kogwerks.mash.dto.ConnectionPropertyDto;
import com.kogwerks.mash.dto.TableProfileDto;
import com.kogwerks.mash.factory.ConnectorFactory;
import com.kogwerks.mash.service.DataConnector;
import com.kogwerks.mash.service.SparkService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/connector")
@AllArgsConstructor
@Slf4j
@CrossOrigin("*")
public class ConnectorController {

    private Map<String, DataConnector> dataConnectorMap;
    private List<String>               tables;

    private final SparkService sparkService;

    @PostMapping
    public List<String> createConnection(@RequestBody @Valid ConnectionPropertyDto connectionPropertyDto) {
        if (dataConnectorMap.size() == 4) {
            log.info("No. of active connections {}", dataConnectorMap.size());
            return tables;
        }
        // Create SQL connector
        // DataConnector sqlConnector = ConnectorFactory.createConnector("sql");
        DataConnector sqlConnector = ConnectorFactory.createConnector(connectionPropertyDto.getType());
        tables = sqlConnector.connect();

        dataConnectorMap.put(connectionPropertyDto.getType(), sqlConnector);
        return tables;
    }

    @GetMapping("/job")
    public TableProfileDto runJob() {
        return sparkService.runJob();
    }

 /*   @GetMapping("/schema")
    public void getSchema() {
        sparkService.getSchema("mytable");
    }*/

}
