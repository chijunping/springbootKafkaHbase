package com.zhibo8.warehouse.service;

import java.util.List;
import java.util.Map;

public interface IClickEventService {
    List<Map<String,Object>> getClickEventsByUDID(String udid);

    List<Map<String,Object>> getClickEventsByRowkeys(List<String> rowkeys);
}

