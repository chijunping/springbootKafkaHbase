package com.zhibo8.warehouse.commons;

import com.zhibo8.warehouse.entity.Comment;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.CommentRowkeyBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HBase工具类
 */
public class HBaseUtil implements Serializable {

    // 日志记录器
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    // HBase配置
    private static final int DEFAULT_MAX_VERSIONS = 3;
    private static Configuration conf;

    //是否关闭
    private static AtomicBoolean isClosed = new AtomicBoolean(false);
    //队列实现连接 对象存储
    private static LinkedBlockingQueue<Connection> idle; //空闲队列
    private static LinkedBlockingQueue<Connection> busy; //繁忙队列
    //大小控制连接数量
    private static AtomicInteger currPoolSize = new AtomicInteger(0);
    //记录连接被创建的次数
    private static AtomicInteger createCounter = new AtomicInteger(0);
    //连接池连接资源数量，等待时间
    private static int maxPoolSize;
    private static long maxWaitTime;

    @Value("${hbase.scan.caching}")
    private static int caching;

//    #################################### 连接池操作 #############################################################

    /**
     * 连接池：初始化
     *
     * @param maxPoolSize_
     * @param maxWaitTime_
     * @param zkHost
     */
    public static void init(int initialSize, int maxPoolSize_, long maxWaitTime_, String zkHost) { //maxActive 允许最大并发数（最大连接数）、maxWait 允许最大等待时间
        maxPoolSize = maxPoolSize_;
        maxWaitTime = maxWaitTime_;
        idle = new LinkedBlockingQueue<Connection>();
        busy = new LinkedBlockingQueue<Connection>();
        if (conf == null) {
            conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, zkHost);
            conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
        }
        //初始化创建 initialSize 个连接资源
        Connection conn = null;
        for (int i = 0; i < initialSize; i++) {
            try {
                conn = createConnection();
                //存入busy队列
                idle.offer(conn);
                //当前连接池大小计数器+1
                currPoolSize.incrementAndGet();

                int sizeOfBusyPool = busy.size();
                int sizeOfIdelPool = idle.size();
                logger.info("初始化创建initialSize 个连接：工作池大小=" + sizeOfBusyPool + "=====空闲池大小=" + sizeOfIdelPool + "===当前连接池大小=" + currPoolSize);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 连接池：获得连接资源
     *
     * @return
     * @throws Exception
     */
    public static Connection getConnection() throws Exception {
        Connection conn = null;
        long borrowStartTime = System.currentTimeMillis();//获取连接的开始时间

        while (null == conn) {
            //1.尝试从空闲队列中获取一个连接
            conn = idle.poll();
            //2.如果空闲队列里有连接,直接返回该连接资源供使用，并将此连接移动到busy （繁忙）队列中
            if (conn != null) {
                busy.offer(conn);
                int sizeOfBusyPool = busy.size();
                int sizeOfIdelPool = idle.size();
                logger.info("从空闲队列里拿到连接,工作池大小=" + sizeOfBusyPool + "=====空闲池大小=" + sizeOfIdelPool + "===当前连接池大小=" + currPoolSize);
                return conn;
            }
            //3.如果空闲队列无资源，且当前连接池大小<maxPoolSize,则手动创建新资源，并放入busy队列
            if (currPoolSize.incrementAndGet() <= maxPoolSize) {
                //创建 Connection 连接
                conn = createConnection();
                //存入busy队列
                busy.offer(conn);
                int sizeOfBusyPool = busy.size();
                int sizeOfIdelPool = idle.size();
                logger.info("连接被创建的次数：" + createCounter.incrementAndGet() + "===工作池大小=" + sizeOfBusyPool + "=====空闲池大小=" + sizeOfIdelPool + "===当前连接池大小=" + currPoolSize);
                return conn;
            } else {
                //4.加完后,如果超出大小再减回来，超过连接池大小—那么不创建，等待别人释放资源；
                //4.1 currPoolSize-1
                currPoolSize.decrementAndGet();
                //4.2 判断是否超时，超市则抛出异常
                long alreadyWaitTime = System.currentTimeMillis() - borrowStartTime;
                if (alreadyWaitTime >= maxWaitTime) {
                    throw new Exception("获取Hbase Connection 连接资源时，超出等待时间 ... ");
                }
                //4.3 不超时，则继续等待别人的资源释放
                try {
                    //等待别人释放得到连接，同时也有最长的等待时间限制
                    conn = idle.poll(maxWaitTime - (System.currentTimeMillis() - borrowStartTime), TimeUnit.MILLISECONDS);
                    busy.offer(conn);
                    int sizeOfBusyPool = busy.size();
                    int sizeOfIdelPool = idle.size();
                    logger.info("成功拿到等待资源===工作池大小=" + sizeOfBusyPool + "=====空闲池大小=" + sizeOfIdelPool + "===当前连接池大小=" + currPoolSize);
                    return conn;
                } catch (Exception e) {
                    throw new Exception("获取Hbase Connection 连接资源时，等待资源期间发生异常 ... " + e);
                }
            }

        }
        return conn;
    }


    /**
     * 连接池：释放资源
     *
     * @param conn
     */
    public static void releaseConnection(Connection conn) {
        try {
            release(conn);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 连接池：释放资源
     *
     * @param conn
     * @throws Exception
     */
    private static void release(Connection conn) throws Exception {
        if (null == conn) {
            logger.warn("释放 的 Connection 为空");
            return;
        }
        if (busy.remove(conn)) {
            idle.offer(conn);
            int sizeOfBusyPool = busy.size();
            int sizeOfIdelPool = idle.size();
            logger.info("释放资源：" + createCounter.incrementAndGet() + "===工作池大小=" + sizeOfBusyPool + "=====空闲池大小=" + sizeOfIdelPool + "===当前连接池大小=" + currPoolSize);
        } else {
            //如果释放不成功,则减去一个连接，在创建的时候可以自动补充
            currPoolSize.decrementAndGet();
            throw new Exception("释放 Connection 异常");
        }
    }

    /**
     * 连接池：
     */
    public static void close() {

        if (isClosed.compareAndSet(false, true)) {
            LinkedBlockingQueue<Connection> pool = idle;
            while (pool.isEmpty()) {
                Connection conn = pool.poll();
                HBaseUtil.closeConnect(conn);
                if (pool == idle && pool.isEmpty()) {
                    pool = busy;
                }
            }
        }
    }

    /**
     * 连接池：创建按连接资源
     *
     * @return
     */
    private static Connection createConnection() {
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭连接(建议使用release(Connnection conn)方法)
     *
     * @throws IOException
     */
    public static void closeConnect(Connection conn) {
        if (null != conn) {
            try {
                conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }
    }
//################################# 表操作 #########################################################################

    /**
     * 获取 HBaseAdmin 对象，建表等操作
     *
     * @return
     * @throws IOException
     * @author
     */
    public static HBaseAdmin getHBaseAdmin(Connection conn) {
        HBaseAdmin admin = null;
        try {
            admin = (HBaseAdmin) conn.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return admin;
    }

    /**
     * 获取HTable对象
     *
     * @param tableName
     * @return
     * @throws IOException
     * @author
     */
    public static HTable getHTable(Connection conn, String tableName) {
        HTable table = null;
        try {
            table = (HTable) conn.getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 检测 HBase 服务是否可用
     *
     * @return
     * @author
     */
    public static boolean isHBaseAvailable() {
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (ZooKeeperConnectionException e) {
            logger.error("zookeeper连接异常：", e);
            return false;
        } catch (MasterNotRunningException e) {
            logger.error("Hbase master为运行异常：", e);
            return false;
        } catch (Exception e) {
            logger.error("Check HBase available throws an Exception. We don't know whether HBase is running or not.", e);
            return false;
        }
        return true;
    }


    public static void closeAdmin(Admin admin) {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭HTable对象
     *
     * @param table
     * @author
     */
    public static void closeTable(Table table) {
        if (table == null) {
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表：指定分区数量、最大版本数
     *
     * @param tableName
     * @param regionNum
     * @param regionNum
     * @param families
     * @throws IOException
     */
    public static void createTable(String tableName, int regionNum, int maxVersions, String... families) {
        byte[][] splitKeys = genSplitKeys(regionNum);
        createTable(tableName, maxVersions, splitKeys, families);
    }

    /**
     * 创建表操作
     *
     * @param tableName
     * @param families
     * @author
     */
    public static void createTable(String tableName, int maxVersions, byte[][] splitKeys, String[] families) {
        // 参数判空
        if (StringUtils.isBlank(tableName) || families == null || families.length <= 0) {
            return;
        }
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            // 表不存在则创建
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
                for (String family : families) {
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                    columnDescriptor.setCompressionType(Algorithm.SNAPPY);
                    //columnDescriptor.setMinVersions(maxVersions);
                    columnDescriptor.setMaxVersions(maxVersions);
                    desc.addFamily(columnDescriptor);
                }
                if (splitKeys != null) {
                    admin.createTable(desc, splitKeys);
                } else {
                    admin.createTable(desc);
                }
            } else {
                logger.warn("Table " + tableName + " already exists.");
            }
        } catch (Exception e) {
            logger.error("建表异常：\n", e.getMessage());
        } finally {
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @author
     */
    public static void dropTable(String tableName) {
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                admin.disableTable(TableName.valueOf(tableName));
                admin.deleteTable(TableName.valueOf(tableName));
            }
        } catch (Exception e) {
            logger.error("删表异常：\n" + e.getMessage());
        } finally {
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }

    /**
     * 清空表
     */
    public static void truncateTable(String tableName_) {
        Connection conn = null;
        HBaseAdmin admin = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            // 取得目标数据表的表名对象
            TableName tableName = TableName.valueOf(tableName_);
            // 设置表状态为无效
            admin.disableTable(tableName);
            // 清空指定表的数据
            admin.truncateTable(tableName, true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }

    /**
     * 创建命名空间
     *
     * @param conn
     * @param namespace
     * @param author
     */
    public static void initNamespace(Connection conn, String namespace, String author) {
        HBaseAdmin admin = getHBaseAdmin(conn);
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", author)
                .build();
        try {
            admin.createNamespace(nd);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeAdmin(admin);
            closeConnect(conn);
        }
    }


    /**
     * 生成预分区 区号
     *
     * @param regionNum
     * @return
     */
    private static byte[][] genSplitKeys(int regionNum) {
        //定义一个存放分区键的数组
        String[] keys = new String[regionNum];
        //目前推算，region个数不会超过2位数，所以region分区键格式化为两位数字所代表的字符串
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regionNum; i++) {
            keys[i] = df.format(i) + "|";
        }

        byte[][] splitKeys = new byte[regionNum][];
        //生成byte[][]类型的分区键的时候，一定要保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regionNum; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while (splitKeysIterator.hasNext()) {
            byte[] b = splitKeysIterator.next();
            splitKeys[index++] = b;
        }
        return splitKeys;
    }


//    ##################################### 数据操作 ##############################################

    /**
     * 查询：获取单个列值: byte[] 返回
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author
     */
    public static byte[] getCell(Connection conn, String tableName, String rowkey, String family, String qualifier) {
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            table = getHTable(conn, tableName);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                return result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else {
                logger.warn("表 " + tableName + " 不存在。");
            }
        } catch (Exception e) {
            logger.error("获取列值失败！ \n" + e);
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
        return null;
    }

    /**
     * 查询：获取单个列值，字符串返回
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author zhanglei11
     */
    public static String getCellString(Connection conn, String tableName, String rowkey, String family, String qualifier) {
        return Bytes.toString(getCell(conn, tableName, rowkey, family, qualifier));
    }

    /**
     * 查询：获取一行中某列族的值
     *
     * @param tableName
     * @param rowkey
     * @return
     * @author
     */
    public static Map<String, byte[]> getMapByKeyAndFamily(Connection conn, String tableName, String rowkey, String family) {
        Map<String, byte[]> map = new HashMap<String, byte[]>();
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Get get = new Get(Bytes.toBytes(rowkey));
                get.addFamily(Bytes.toBytes(family));
                Result result = table.get(get);
                for (Cell cell : result.rawCells()) {
                    byte[] q = CellUtil.cloneQualifier(cell);
                    byte[] v = CellUtil.cloneValue(cell);
                    map.put(Bytes.toString(q), v);
                }
            } else {
                logger.warn("表 " + tableName + " 不存在。");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
        return map;
    }


    /**
     * 查询数据集合
     *
     * @param tableName
     * @param rowkeys
     * @return List<Map                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               <                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               String                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               Object>>
     */
    public static List<Map<String, Object>> getRowMapsByKeys(String tableName, List<String> rowkeys) {
        List<Map<String, Object>> rsMaps = new ArrayList<>();
        HTable table = null;
        HBaseAdmin admin = null;
        Connection conn = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            table = getHTable(conn, tableName);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                List<Get> gets = toGetsFromRowkeys(rowkeys);
                Result[] results = table.get(gets);
                rsMaps = toMapsFromResultArr(results);
            } else {
                logger.warn("表 " + tableName + " 不存在。");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeTable(table);
            closeAdmin(admin);
            releaseConnection(conn);
        }
        return rsMaps;
    }

    public static List<Get> toGetsFromRowkeys(List<String> rowkeys) {
        List<Get> gets = new ArrayList<>();
        for (String rowkey : rowkeys) {
            Get get = new Get(Bytes.toBytes(rowkey));
            gets.add(get);
        }
        return gets;
    }

    public static List<Map<String, Object>> toMapsFromResultScanner(ResultScanner rsScanner) throws Exception {
        if (rsScanner == null) return null;
        List<Map<String, Object>> recordList = new ArrayList<>();
        Result r = null;
        while ((r = rsScanner.next()) != null) {
            List<Cell> cells = r.listCells();
            Map<String, Object> record = new TreeMap<>();
            if (cells != null && cells.size() > 0) {
                for (int i = 0; i < cells.size(); i++) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)));
                    String value = Bytes.toString(CellUtil.cloneValue(cells.get(i)));
                    record.put(key, value);
                }
                recordList.add(record);
            }
        }
        return recordList;
    }

    public static List<Map<String, Object>> toMapsFromResultArr(Result[] results) throws Exception {
        if (results == null && results.length == 0) return null;
        List<Map<String, Object>> recordList = new ArrayList<>();
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            Map<String, Object> record = new TreeMap<>();
            if (cells != null && cells.size() > 0) {
                for (int i = 0; i < cells.size(); i++) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)));
                    String value = Bytes.toString(CellUtil.cloneValue(cells.get(i)));
                    record.put(key, value);
                }
                recordList.add(record);
            }
        }
        return recordList;
    }

    /**
     * 查询：获取多行记录
     *
     * @param tableName
     * @param rowkeys
     * @return
     */
    public static Result[] getRowsByKeys(Connection conn, String tableName, List<String> rowkeys) {
        if (rowkeys == null || rowkeys.size() == 0) {
            logger.warn("Has no rowkeys to get.");
            return null;
        }
        List<Get> gets = new ArrayList<>();
        Get get = null;
        for (String rowkey : rowkeys) {
            get = new Get(Bytes.toBytes(rowkey));
            gets.add(get);
        }

        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            table = getHTable(conn, tableName);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Result[] results = table.get(gets);
                return results;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return null;
            }
        } catch (Exception e) {
            logger.error("查询数据时异常：", tableName, e);
            return null;
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }

    }

    /**
     * 查询：获取一条记录
     *
     * @param tableName
     * @param rowkey
     * @return
     */
    public static Result getRowByKey(Connection conn, String tableName, String rowkey) {
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            table = getHTable(conn, tableName);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                return result;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return null;
            }
        } catch (IOException e) {
            logger.error("查询数据时异常：", tableName, e);
            return null;
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
    }

    /**
     * 查询：获取多行记录
     *
     * @param tableName
     * @param gets
     * @return
     */
    public static Result[] getRowsByGets(Connection conn, String tableName, List<Get> gets) {
        if (gets == null || gets.size() == 0) {
            logger.warn("Has no gets to get.");
            return null;
        }
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            table = getHTable(conn, tableName);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Result[] results = table.get(gets);
                return results;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return null;
            }
        } catch (Exception e) {
            logger.error("查询数据时异常：", tableName, e);
            return null;
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
    }

    /**
     * 查询：获取一条记录
     *
     * @param tableName
     * @param get
     * @return
     */
    public static Result getRowByGet(Connection conn, String tableName, Get get) {
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            table = getHTable(conn, tableName);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                Result result = table.get(get);
                return result;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return null;
            }
        } catch (Exception e) {
            logger.error("查询数据时异常：", tableName, e);
            return null;
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
    }

    /**
     * 查询：根据 rowkey 前缀匹配
     *
     * @param conn
     * @param tableName
     * @param rowkeyPrefix
     * @return
     */
    public static ResultScanner queryByPrefix(Connection conn, String tableName, String rowkeyPrefix) {
        ResultScanner scanner = null;// 通过scan查询结果
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Scan scan = new Scan();// 创建scan，用于查询
                PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowkeyPrefix));// 创建正则表达式filter
                //RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(rowkeyPrefix)));
                scan.setFilter(filter);// 设置filter
                scan.setCaching(caching); //默认本地缓存100条数据，大scan可以设置为1000
                scanner = table.getScanner(scan);
            } else {
                logger.warn("表 " + tableName + " 不存在。");
            }
        } catch (IOException e) {
            logger.error("查询数据时异常：", tableName, e);
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
        return scanner;
    }

    /**
     * 查询：根据 rowkey 正则表达式匹配
     *
     * @param conn
     * @param tableName
     * @param reg
     * @return
     */
    public static ResultScanner queryByReg(Connection conn, String tableName, String reg) {
        ResultScanner scanner = null;// 通过scan查询结果
        HTable table = null;
        HBaseAdmin admin = null;
        try {
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Scan scan = new Scan();// 创建scan，用于查询
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(reg));// 创建正则表达式filter
                scan.setFilter(filter);// 设置filter
                scan.setCaching(caching); //默认本地缓存100条数据，大scan可以设置为1000
                scanner = table.getScanner(scan);
            } else {
                logger.warn("表 " + tableName + " 不存在。");
            }
        } catch (IOException e) {
            logger.error("查询数据时异常：", tableName, e);
        } finally {
            closeTable(table);
            closeAdmin(admin);
        }
        return scanner;
    }

    /**
     * 新增：新增 cell
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param quailifer
     * @param value
     * @return
     */
    public static boolean insertCell(String tableName, String rowKey, String family, String quailifer, String value) {
        HTable table = null;
        HBaseAdmin admin = null;
        Connection conn = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Put put = new Put(rowKey.getBytes());
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(quailifer), Bytes.toBytes(value));
                table.put(put);
                return true;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            closeTable(table);
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }

    /**
     * 新增：新增一行
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param rowMap
     * @return
     */
    public static boolean insertRow(String tableName, String rowKey, String family, Map<String, Object> rowMap) {
        HTable table = null;
        HBaseAdmin admin = null;
        Connection conn = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Put put = new Put(rowKey.getBytes());
                for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
                }
                table.put(put);
                return true;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            closeTable(table);
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }

    /**
     * 新增：新增一行的多个 cells
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param rowMap
     * @return
     */
    public static boolean insertRowBatch(String tableName, String rowKey, String family, Map<String, Object> rowMap) {
        HTable table = null;
        HBaseAdmin admin = null;
        Connection conn = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                Put put = new Put(rowKey.getBytes());
                for (Map.Entry<String, Object> entry : rowMap.entrySet()) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
                }
                table.put(put);
                return true;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            closeTable(table);
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }


    /**
     * 新增：新增多行
     *
     * @param tableName
     * @param family
     * @param rows      [[rowkey,map<字段名,字段值>],...]
     * @return
     */
    public static boolean insertRows(String tableName, String family, List<List<Object>> rows, long writeBufferSize) {
        List<Put> puts = new ArrayList<>();
        HTable table = null;
        HBaseAdmin admin = null;
        Connection conn = null;
        try {
            conn = getConnection();
            admin = getHBaseAdmin(conn);
            if (admin.tableExists(TableName.valueOf(tableName))) {
                table = getHTable(conn, tableName);
                table.setAutoFlushTo(false);
                table.setWriteBufferSize(writeBufferSize); //5M
                for (List<Object> row : rows) {
                    String rowkey = String.valueOf(row.get(0)); //
                    Map<String, Object> paramMap = (HashMap<String, Object>) row.get(1);
                    Put put = new Put(Bytes.toBytes(rowkey));
                    for (Map.Entry<String, Object> entry : paramMap.entrySet()) {
                        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(entry.getKey()), Bytes.toBytes(String.valueOf(entry.getValue())));
                    }
                    puts.add(put);
                }
                table.put(puts);
                table.flushCommits();
                return true;
            } else {
                logger.warn("表 " + tableName + " 不存在。");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            closeTable(table);
            closeAdmin(admin);
            releaseConnection(conn);
        }
    }


    /**
     * 删除多行数据
     *
     * @param tablename
     * @param rowKeys
     * @throws IOException
     */
    public static void deleteRow(String tablename, List<String> rowKeys) throws Exception {
        Connection conn = getConnection();
        HTable table = getHTable(conn, tablename);
        if (table != null) {
            try {
                List<Delete> list = new ArrayList<Delete>();
                for (String rowKey : rowKeys) {
                    Delete d = new Delete(rowKey.getBytes());
                    list.add(d);
                }
                if (list.size() > 0) {
                    table.delete(list);
                }
            } finally {
                closeTable(table);
                releaseConnection(conn);
            }
        }
    }

    public static void main(String[] args) {

    }

}