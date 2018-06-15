package com.zhibo8.warehouse.service.impl;

import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.ICommentDao;
import com.zhibo8.warehouse.service.PingLunService;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PingLunServiceImpl implements PingLunService {
    @Autowired
    private ICommentDao ICommentDao;

    @Override
    public boolean insertCell(String tableName, String rowKey, String family, String quailifer, String value) {
        return ICommentDao.insertCell(tableName, rowKey, family, quailifer, value);
    }

    @Override
    public boolean insertRow(String tableName, String rowKey, String family, Map<String, Object> rowMap) {
        return ICommentDao.insertRow(tableName, rowKey, family, rowMap);
    }


    @Override
    public List<Map<String,Object>> getRowsByArticleId(String userId_) {
        List<Map<String, Object>> recordList = new ArrayList<>();
        Connection conn =null;
        try {
            conn = HBaseUtil.getConnection();
            ResultScanner rsScanner= ICommentDao.queryByPrefix(conn , Constants.COMMENT_TABLE_NAME,userId_);
            for (Result row : rsScanner) {// 循环结果集
                HashMap<String, Object> recordMap = new HashMap<>(); //用来封装解析后的row
                String rowKey = new String(row.getRow());// rowKey
                String userId = Bytes.toString(row.getValue(Bytes.toBytes(Constants.COMMENT_FAMILY), Bytes.toBytes(Constants.COMMENT_USERID)));
                String parentId = Bytes.toString(row.getValue(Bytes.toBytes(Constants.COMMENT_FAMILY), Bytes.toBytes(Constants.COMMENT_PARENTID)));
                String content = Bytes.toString(row.getValue(Bytes.toBytes(Constants.COMMENT_FAMILY), Bytes.toBytes(Constants.COMMENT_CONTENT)));
                String createTime =Bytes.toString(row.getValue(Bytes.toBytes(Constants.COMMENT_FAMILY), Bytes.toBytes(Constants.COMMENT_CREATETIME)));
                String status =Bytes.toString(row.getValue(Bytes.toBytes(Constants.COMMENT_FAMILY), Bytes.toBytes(Constants.COMMENT_STATUS)));
                recordMap.put("rowKey", rowKey);
                recordMap.put("userId", userId);
                recordMap.put("parentId", parentId);
                recordMap.put("content", content);
                recordMap.put("createTime", createTime);
                recordMap.put("status", status);
                recordList.add(recordMap);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
          HBaseUtil.releaseConnection(conn);
        }
        return recordList;
    }



}
