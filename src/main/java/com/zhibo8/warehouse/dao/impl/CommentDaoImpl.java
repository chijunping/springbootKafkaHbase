package com.zhibo8.warehouse.dao.impl;

import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.ICommentDao;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * 表设计原则：
 * columfamily 尽量一张表就一个 family
 * rowkey
 * >> 长度：长度<100字节，越短越好
 * >> 结构：所属者id_timestemp_自身id(有从属者时加自身id)
 */
@Repository
public class CommentDaoImpl implements ICommentDao {

    @Autowired
    private Environment env;
    /**
     * 新增行，该行包含一个cell
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param quailifer
     * @param value
     * @return
     */
    @Override
    public boolean insertCell(String tableName, String rowKey, String family, String quailifer, String value) {
        return HBaseUtil.insertCell(tableName, rowKey, family, quailifer, value);
    }

    /**
     * 新增行，该行可包含任意cell （往同一个columFamily 中新增cells）
     * rowMap 示例 ：{"userid":"u_1001","age":"12"}
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param rowMap
     */
    @Override
    public boolean insertRow(String tableName, String rowKey, String family, Map<String, Object> rowMap) {
//        return HBaseUtil.insertRow(tableName, rowKey, family, rowMap);
        return HBaseUtil.insertRowBatch(tableName, rowKey, family, rowMap);
    }

    /**
     * 添加多行
     *
     * @param tableName
     * @param family
     * @param rows [[rowkey,Map<column,value>],...]
     */
    @Override
    public boolean insertRows(String tableName, String family, List<List<Object>> rows) {
        long  writeBufferSize = Long.valueOf(env.getProperty("hbase.writeBufferSize"));
        return HBaseUtil.insertRows(tableName, family, rows,writeBufferSize);
    }

    /**
     * 查询：通过 rowKey 正则表达式
     * tableName：表名
     * reg：正则表达式
     * ResultScanner：结果集
     * IOException：IO异常
     */
    @Override
    public ResultScanner queryByReg(Connection conn, String tableName, String reg) {
        return HBaseUtil.queryByReg(conn, tableName, reg);
    }

    /**
     * 查询：根据 rowkey 前缀
     *
     * @param tableName
     * @param rowkeyPrefix
     * @return
     */
    @Override
    public ResultScanner queryByPrefix(Connection conn, String tableName, String rowkeyPrefix) {
        return HBaseUtil.queryByPrefix(conn, tableName, rowkeyPrefix);
    }


}
