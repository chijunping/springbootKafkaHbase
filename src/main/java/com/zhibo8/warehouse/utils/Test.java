package com.zhibo8.warehouse.utils;

import java.util.concurrent.CountDownLatch;


public class Test {

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
//                            e.printStackTrace();
//                        } finally {
//                            try {
//                                pool.release(conn); //释放连接
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }
//
//                }
//            }).start();
//        }
    }

}