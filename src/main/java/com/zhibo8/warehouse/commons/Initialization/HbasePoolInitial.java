package com.zhibo8.warehouse.commons.Initialization;

import com.zhibo8.warehouse.commons.HBaseUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * CommandLineRunner 接口的 Component 会在所有 Spring Beans 都初始化之后，SpringApplication.run() 之前执行，
 * 非常适合在应用程序启动之初进行一些数据初始化的工作。
 * <p>
 * hbase.zk_quorum=hb-proxy-pub-xxxxxx-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-xxxxxx-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-xxxxxx-003.hbase.rds.aliyuncs.com:2181
 * hbase.zk_parent=/hbase
 * hbase.writeBufferSize=5242880 #1024*1024*5  5M
 * hbase.pool.initialSize=1
 * hbase.pool.maxWait=60000
 * hbase.pool.minIdle=1
 * hbase.pool.maxActive=170
 */
@Component
public class HbasePoolInitial implements CommandLineRunner {
    @Value("${hbase.zk_quorum}")
    private String zkHost;
    @Value("${hbase.pool.initialSize}")
    private Integer initialSize;
    @Value("${hbase.pool.minIdle}")
    private Integer minIdle;
    @Value("${hbase.pool.maxActive}")
    private Integer maxActive;
    @Value("${hbase.pool.maxWait}")
    private Long maxWait;

    @Override
    public void run(String... args){
        HBaseUtil.init(initialSize,maxActive, maxWait, zkHost);
    }
}