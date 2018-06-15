package com.zhibo8.warehouse.service;


import java.util.List;
import java.util.Map;

public interface PingLunService {
    boolean insertCell(String tableName, String rowKey, String family, String quailifer, String value);

    boolean insertRow(String tableName, String rowKey, String family, Map<String, Object> rowMap);


    List<Map<String,Object>> getRowsByArticleId(String articleId);
}
