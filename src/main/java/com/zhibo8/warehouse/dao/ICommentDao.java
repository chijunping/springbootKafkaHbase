package com.zhibo8.warehouse.dao;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ICommentDao {
    boolean insertCell(String tableName, String rowKey, String family, String quailifer, String value);

    boolean insertRow(String tableName, String rowKey, String family, Map<String, Object> rowMap);


    ResultScanner queryByReg(Connection conn, String tableName, String articleId);

    ResultScanner queryByPrefix(Connection conn, String tableName, String rowkeyPrefix);
    /**
     * 添加多行
     *
     * @param tableName
     * @param family
     * @param rows [[rowkey,Map<column,value>],...]
     */
    boolean insertRows(String tableName, String family, List<List<Object>> rows) throws IOException;
}
