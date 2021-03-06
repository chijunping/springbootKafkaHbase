package com.zhibo8.warehouse.dao.impl;

import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.IAdLogDao;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.ClickRowkeyBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class AdLogDaoImpl implements IAdLogDao {

    @Override
    public ResultScanner queryByPrefix(Connection conn, String tableName, String udid) {
        String regionCode = ClickRowkeyBuilder.buildRegionCode(udid, Constants.CLICK_REGINNUM);
        String rowkeyPrefix = regionCode + "_" + udid;
        return HBaseUtil.queryByPrefix(conn, tableName, rowkeyPrefix);
    }

    @Override
    public List<Map<String, Object>> getClickEventsByRowkeys(List<String> rowkeys, String tableName) {
        List<Map<String, Object>> rowMaps = HBaseUtil.getRowMapsByKeys(tableName, rowkeys);
        return rowMaps;
    }
}
