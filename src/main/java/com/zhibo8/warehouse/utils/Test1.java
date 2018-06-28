package com.zhibo8.warehouse.utils;

import com.sun.corba.se.impl.presentation.rmi.ExceptionHandlerImpl;
import com.zhibo8.warehouse.commons.config.SystemConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.*;

import static com.zhibo8.warehouse.commons.HBaseUtil.closeAdmin;
import static com.zhibo8.warehouse.commons.HBaseUtil.closeConnect;
import static com.zhibo8.warehouse.commons.HBaseUtil.getHBaseAdmin;


public class Test1 {

    private static int threadCount = 10;

    private final static CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(threadCount); //为保证30个线程同时并发运行

    public static void main(String[] args) {
//        final ConnectionPool_Test pool = new ConnectionPool_Test();
//        String zkHost = "";
//        //连接池最大连接数和获取连接的超时时间
//        pool.init(50, 2000L, zkHost);
//
//        for (int i = 0; i < threadCount; i++) {//循环开30个线程
//            new Thread(new Runnable() {
//
//                public void run() {
//                    int i = 0;
//                    while (i < 10) {//每个线程里循环十次获取连接
//                        i++;
//                        Connection_testPojo conn = null;
//                        try {
//                            COUNT_DOWN_LATCH.countDown();//每次减一
//                            COUNT_DOWN_LATCH.await(); //此处等待状态，为了让30个线程同时进行
//                            conn = pool.borrowResource();
//                        } catch (Exception e) {
//                            logger.error(e.getMessage(), e)();
//                        } finally {
//                            try {
//                                pool.release(conn); //释放连接
//                            } catch (Exception e) {
//                                logger.error(e.getMessage(), e)();
//                            }
//                        }
//                    }
//
//                }
//            }).start();
//        }
    }

    @Test
    public void singleThreadExecutor() {
        ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            singleThreadExecutor.execute(new Runnable() {
                public void run() {
                    try {
                        System.out.println(index);
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            singleThreadExecutor.shutdown();
        }
    }

    @Test
    public void testInteger() {
        int concurrency = Integer.valueOf(null);
        System.out.println(concurrency);
    }

    @Test
    public void ExecutorServiceTest() throws InterruptedException {
        // 创建一个固定大小的线程池
        for (int j = 0; j < 3; j++) {
            ExecutorService service = Executors.newFixedThreadPool(3);

            for (int i = 0; i < 30; i++) {
                int finalI = i;
                // 在未来某个时间执行给定的命令
                service.execute(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("启动线程=" + finalI);
                        System.out.println(Thread.currentThread());
                    }
                });
            }
            service.shutdown();
            // 等待子线程结束，再继续执行下面的代码
//            service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            System.out.println("all thread complete");
        }

    }

    @Test
    public void truncateTable() {
        Configuration config = HBaseConfiguration.create();
        //建立Hbase连接
        String zkAddress = "hb-proxy-pub-bp1987l1fy04etj46-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-003.hbase.rds.aliyuncs.com:2181";
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
            e.printStackTrace();
        } finally {
            closeAdmin(admin);
            closeConnect(conn);
        }
    }


    private void TestLinkedBlockingQueue() throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.println("i=" + i);
            if (i == 2) {
                throw new Exception("异常");
            }
        }
    }

    @Test
    public void testexception() {
        try {
            TestLinkedBlockingQueue();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}