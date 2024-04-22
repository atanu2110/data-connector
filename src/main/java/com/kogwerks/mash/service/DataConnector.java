package com.kogwerks.mash.service;

import java.util.List;

public interface DataConnector {

    List<String> connect();

    void close();
}
