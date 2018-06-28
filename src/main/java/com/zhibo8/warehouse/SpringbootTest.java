package com.zhibo8.warehouse;

import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.ICommentDao;
import com.zhibo8.warehouse.service.impl.PingLunServiceImpl;
import com.zhibo8.warehouse.test.AliHbaseTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static com.zhibo8.warehouse.commons.HBaseUtil.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootTest {
    private static Logger logger = LoggerFactory.getLogger(AliHbaseTest.class);

    @Autowired
    private PingLunServiceImpl pingLunService;
    @Autowired
    private ICommentDao ICommentDao;
    @Value("${hbase.zk_quorum}")
    private String hbase_zookeeper_quorum;
    @Value("${hbase.pool.initialSize}")
    private int initialSize;
    @Value("${hbase.pool.minIdle}")
    private int minIdle;
    @Value("${hbase.pool.maxActive}")
    private int maxActive;

    @Test
    public void testValue() {
        System.out.println("############################" + hbase_zookeeper_quorum);
    }

    @Test
    public void contextLoads() {
        String tableName = "comment";
        String rowKey = "pl_1231233421_" + System.currentTimeMillis();
        String family = "pl";
        String quailifer = "content";
        String value = "你好，单个cell插入测试！";
        pingLunService.insertCell(tableName, rowKey, family, quailifer, value);
    }

    /**
     * 添加 1000 条测试数据
     */
    @Test
    public void insertRows() {
        //{"contentId":"c_07","content":"评论7？","userId":"u_01","parentId":"u_00","articleId":"ar_002","age":32,"createTime":"2012-12-12 12:23:23","status":1}
        String zkAddress = "hb-proxy-pub-bp12mmg4lu45o4ivy-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-003.hbase.rds.aliyuncs.com:2181";

        HBaseUtil.init(1, 50, 2000L, zkAddress);

        Connection conn = null;
        Table table = null;
        try {
            conn = getConnection();
            table = HBaseUtil.getHTable(conn, Constants.COMMENT_TABLE_NAME);
            List<List<Object>> rows = new ArrayList<>();
            List<Object> paramList = null;
            String[] articleIds = {"ar_001", "ar_002", "ar_003", "ar_004", "ar04_005"};
            String[] userIds = {"u_001", "u_002", "u_003", "u_004"};
            for (int i = 0; i < 100; i++) {
                HashMap<String, Object> paramMap = new HashMap<>();
                //传参
                String articleId = articleIds[new Random().nextInt(5)];
                String contentId = "c_" + i;
                String userId = userIds[new Random().nextInt(4)];
                String parentId = userIds[new Random().nextInt(4)];
                int age = i;
                String createTime = "2012-12-12 23:23:23";
                int status = new Random().nextInt(2);
                //构造rowkey
                String rowkey = "pl_" + articleId + "-" + System.currentTimeMillis() + "-" + contentId;
                //构造记录
                paramMap.put("contentId", contentId);
                paramMap.put("articleId", articleId);
                paramMap.put("userId", userId);
                paramMap.put("parentId", parentId);
                paramMap.put("age", age);
                paramMap.put("createTime", createTime);
                paramMap.put("status", status);
                //添加到记录List
                paramList = new ArrayList<>();
                paramList.add(rowkey);
                paramList.add(paramMap);
                rows.add(paramList);
            }
            //批量增则rows
            ICommentDao.insertRows(Constants.COMMENT_TABLE_NAME, Constants.COMMENT_FAMILY, rows);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            HBaseUtil.closeTable(table);
            releaseConnection(conn);
        }
    }


    @Test
    public void insertRow() {
        //{"contentId":"c_07","content":"评论7？","userId":"u_01","parentId":"u_00","articleId":"ar_002","age":32,"createTime":"2012-12-12 12:23:23","status":1}
        String zkAddress = "hb-proxy-pub-bp12mmg4lu45o4ivy-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp12mmg4lu45o4ivy-003.hbase.rds.aliyuncs.com:2181";

        HBaseUtil.init(1, 50, 2000L, zkAddress);
        int threadCount = 30;
        CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(threadCount); //为保证30个线程同时并发运行

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {//循环开30个线程
            int finalI = i;


            new Thread(new Runnable() {

                public void run() {
                    Connection conn = null;
                    Table table = null;
                    String[] articleIds = {"ar_006", "ar_006", "ar_006", "ar_006", "ar04_006"};
                    String[] userIds = {"u_001", "u_002", "u_003", "u_004"};

                    HashMap<String, Object> paramMap = new HashMap<>();
                    //传参
                    String articleId = articleIds[new Random().nextInt(5)];
                    String contentId = "c_" + finalI;
                    String userId = userIds[new Random().nextInt(4)];
                    String parentId = userIds[new Random().nextInt(4)];
                    int age = finalI;
                    String createTime = "2012-12-12 23:23:23";
                    int status = new Random().nextInt(2);
                    //构造rowkey
                    String rowkey = "pl_" + articleId + "-" + System.currentTimeMillis() + "-" + contentId;
                    //构造记录
                    paramMap.put("contentId", contentId);
                    paramMap.put("articleId", articleId);
                    paramMap.put("userId", userId);
                    paramMap.put("parentId", parentId);
                    paramMap.put("age", age);
                    paramMap.put("content", "你好速回复哈返回按时发货是的发货发大师傅打发和啊的符号阿斯蒂芬啊是否还多久啊师傅DHL卡激活发射点立刻返回理发店回复啦是否还得安徽是否ask就分类");
                    paramMap.put("createTime", createTime);
                    paramMap.put("status", status);
                    boolean b = ICommentDao.insertRow(Constants.COMMENT_TABLE_NAME, rowkey, Constants.COMMENT_FAMILY, paramMap);
                    System.out.println("添加成功数：" + finalI);
                }
            }).start();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("用时：" + (endTime - startTime));
    }

    /**
     * 清空表
     */
    @Test
    public void truncateTable() {
        Configuration config = HBaseConfiguration.create();
        //建立Hbase连接
        String zkAddress = "hb-proxy-xxxxxx-002.hbase.rds.aliyuncs.com:2181,hb-proxy-xxxxxx-001.hbase.rds.aliyuncs.com:2181,hb-proxy-xxxxxx-003.hbase.rds.aliyuncs.com:2181";
        config.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        String tableName_ = "bigdata:click_dev";//Constants.CLICK_TABLENAME;
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = ConnectionFactory.createConnection(config);
            admin = getHBaseAdmin(conn);
            // 取得目标数据表的表名对象
            TableName tableName = TableName.valueOf(tableName_);
            // 设置表状态为无效
            admin.disableTable(tableName);
            // 清空指定表的数据
            admin.truncateTable(tableName, true);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            closeAdmin(admin);
            closeConnect(conn);
        }
    }

    //模糊匹配rowkey批量删除
    @Test
    public void batchDelete() throws Exception {
        initHbase();//初始化hbase 连接池
        Connection conn = getConnection();
        ResultScanner scanner = HBaseUtil.queryByPrefix(conn, "comment", "ar_1000");
        List<String> rowkeys = new ArrayList<>();
        for (Result row : scanner) {
            String rowkey = Bytes.toString(row.getRow());
            rowkeys.add(rowkey);
        }
        HBaseUtil.deleteRow("comment", rowkeys);
    }


    private void initHbase() {
        String zkAddress = "hb-proxy-xxxxxx-002.hbase.rds.aliyuncs.com:2181,hb-proxy-xxxxxx-001.hbase.rds.aliyuncs.com:2181,hb-proxy-xxxxxx-003.hbase.rds.aliyuncs.com:2181";
        HBaseUtil.init(1, 50, 2000L, zkAddress);
    }

    @Test
    public void initNamespace() throws Exception {
        initHbase();
        HBaseUtil.initNamespace(HBaseUtil.getConnection(), Constants.NS_BIGDATA, Constants.NS_AUTHOR);
    }

    @Test
    public void createTable() {
        initHbase();
//        HBaseUtil.createTable(HBaseUtil.getConnection(),Constants.COMMENT_TABLE_NAME,50,Constants.COMMENT_FAMILY);
//        HBaseUtil.createTable(HBaseUtil.getConnection(),Constants.COMMENT_TABLE_NAME,50,Constants.COMMENT_FAMILY);
//        HBaseUtil.createTable(Constants.CLICK_TABLENAME, Constants.CLICK_REGINNUM,Constants.CLICK_VERSIONNUM, Constants.CLICK_FAMILY);
//        HBaseUtil.createTable("bigdata:click", Constants.CLICK_REGINNUM, Constants.CLICK_VERSIONNUM, Constants.CLICK_FAMILY);
//        HBaseUtil.createTable("bigdata:click_dev", Constants.CLICK_REGINNUM, Constants.CLICK_VERSIONNUM, Constants.CLICK_FAMILY);
        HBaseUtil.createTable("bigdata:ad", Constants.AD_REGINNUM, Constants.AD_VERSIONNUM, Constants.AD_FAMILY);
        HBaseUtil.createTable("bigdata:ad_dev", Constants.AD_REGINNUM, Constants.AD_VERSIONNUM, Constants.AD_FAMILY);
    }

    @Test
    public void dropTable() {
        initHbase();
        // HBaseUtil.dropTable(Constants.COMMENT_TABLE_NAME);
        //HBaseUtil.dropTable("comment");
        //HBaseUtil.dropTable("index");
        HBaseUtil.dropTable("bigdata:click");
    }


}