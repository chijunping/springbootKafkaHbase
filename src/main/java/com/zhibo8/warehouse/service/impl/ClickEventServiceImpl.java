package com.zhibo8.warehouse.service.impl;

import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.IClickEventDao;
import com.zhibo8.warehouse.service.IClickEventService;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class ClickEventServiceImpl implements IClickEventService {


    @Autowired
    IClickEventDao clickEventDao;

    @Value("${hbase.table.click}")
    private String clickTableName;

    @Override
    public List<Map<String, Object>> getClickEventsByUDID(String udid) {
        List<Map<String, Object>> recordList = new ArrayList<>();
        Connection conn = null;
        try {
            conn = HBaseUtil.getConnection();
            ResultScanner rsScanner = clickEventDao.queryByPrefix(conn, clickTableName, udid);
            recordList = HBaseUtil.toMapsFromResultScanner(rsScanner);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.releaseConnection(conn);
        }
        return recordList;
    }

    @Override
    public List<Map<String, Object>> getClickEventsByRowkeys(List<String> rowkeys) {
        List<Map<String, Object>> rowMaps = clickEventDao.getClickEventsByRowkeys(rowkeys, clickTableName);
        return rowMaps;
    }
}
