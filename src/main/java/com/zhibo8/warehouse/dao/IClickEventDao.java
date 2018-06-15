package com.zhibo8.warehouse.dao;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.util.List;
import java.util.Map;

public interface IClickEventDao {
    void insertRows(String commentTableName, String commentFamily, List<List<Object>> rows);

    ResultScanner queryByPrefix(Connection conn, String commentTableName, String udid);

    List<Map<String, Object>> getClickEventsByRowkeys(List<String> rowkeys, String tableName);
}
