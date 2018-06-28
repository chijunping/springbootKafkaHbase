package com.zhibo8.warehouse.test;

import com.zhibo8.warehouse.commons.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.zhibo8.warehouse.commons.HBaseUtil.*;


public class AliHbaseTest {
    private static Logger logger = LoggerFactory.getLogger(AliHbaseTest.class);

    private static final String TABLE_NAME = "mytable2";
    private static final String CF_DEFAULT = "cf";
    public static final byte[] QUALIFIER = "col1".getBytes();
    private static final byte[] ROWKEY = "rowkey1".getBytes();

    public static void main(String[] args) {
        //建立Hbase连接
        Configuration config = HBaseConfiguration.create();
        String zkAddress = "hb-proxy-pub-bp1987l1fy04etj46-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-003.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        config.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "300000");
        config.set(HConstants.HBASE_META_SCANNER_CACHING, "1000");
        config.set(HConstants.HBASE_CLIENT_SCANNER_CACHING, "1000");
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            //创建Hbase中的表结构
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT));
            System.out.print("Creating table. ");
            Admin admin = connection.getAdmin();
            admin.createTable(tableDescriptor);
            System.out.println(" Done.");
            //获取Hbase中的已创建的表对象
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            try {
                //往Hbase中写数据
                Put put = new Put(ROWKEY);
                put.addColumn(CF_DEFAULT.getBytes(), QUALIFIER, "this is value".getBytes());
                table.put(put);
                //从Hbase中读数据
                Get get = new Get(ROWKEY);
                Result r = table.get(get);
                byte[] b = r.getValue(CF_DEFAULT.getBytes(), QUALIFIER);  // returns current version of value
                System.out.println(new String(b));
            } finally {
                if (table != null) table.close();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public static void addTableCoprocessor(String tableName, String coprocessorClassName) {
        Configuration config = HBaseConfiguration.create();
        //建立Hbase连接
        String zkAddress = "hb-proxy-pub-bp12mmg4lu45o4ivy-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-003.hbase.rds.aliyuncs.com:2181";
        String zk = "47.97.35.217:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zk);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            HBaseAdmin admin = getHBaseAdmin(connection);
            admin.disableTable(tableName);
            HTableDescriptor htd = admin.getTableDescriptor(Bytes.toBytes(tableName));
            htd.addCoprocessor(coprocessorClassName);
            admin.modifyTable(Bytes.toBytes(tableName), htd);
            admin.enableTable(tableName);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            HBaseUtil.closeConnect(connection);
        }
    }

    public static long rowCount(String tableName, String family) {
        String zkAddress = "hb-proxy-pub-bp12mmg4lu45o4ivy-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-003.hbase.rds.aliyuncs.com:2181";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        long rowCount = 0;
        try {
            rowCount = ac.rowCount(TableName.valueOf(tableName), new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            logger.error(e.getMessage(), e);
        }
        return rowCount;
    }

    public static long rowCount(Connection conn, String tableName) {
        long rowCount = 0;
        try {
            ResultScanner resultScanner = queryByReg(conn, tableName);
            for (Result result : resultScanner) {
                rowCount += result.size();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return rowCount;
    }

    public static ResultScanner queryByReg(Connection conn, String tableName) {
        ResultScanner scanner = null;// 通过scan查询结果
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Scan scan = new Scan();// 创建scan，用于查询
                scanner = table.getScanner(scan);
            } else {
                System.out.println("Table " + tableName + " does not exist.");
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
        return scanner;
    }

    @Test
    public void testTableRowCount() {
        String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        addTableCoprocessor("comment", coprocessorClassName);
        long rowCount = rowCount("comment", "pl");
        System.out.println("rowCount: " + rowCount);
    }

    @Test
    //计算表行数，为什么增加一条记录，行数加8？？？
    public void testTableRowCount2() {
        Configuration config = HBaseConfiguration.create();
        //建立Hbase连接
        String zkAddress = "hb-proxy-pub-bp1987l1fy04etj46-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-003.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
            long rowCount = rowCount(connection, "comment");
            System.out.println("rowCount: " + rowCount);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            closeConnect(connection);
        }
    }

    @Test
    public void testHBase() throws Exception {
        //建立Hbase连接
        Configuration config = HBaseConfiguration.create();
        String zkAddress = "hb-proxy-pub-bp1987l1fy04etj46-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-003.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        config.set(HConstants.HBASE_RPC_TIMEOUT_KEY, "300000");
        config.set(HConstants.HBASE_META_SCANNER_CACHING, "1000");
        config.set(HConstants.HBASE_CLIENT_SCANNER_CACHING, "1000");
        Connection conn = ConnectionFactory.createConnection(config);

    }

}
