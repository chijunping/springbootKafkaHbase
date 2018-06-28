package com.zhibo8.warehouse.kafka.consumer;

import ch.qos.logback.core.util.TimeUtil;
import com.alibaba.fastjson.JSON;
import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.commons.config.SystemConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class HbaseMsgSender {
    private static Logger logger = LoggerFactory.getLogger(HbaseMsgSender.class);


    /**
     * 批量消费 kafka 的topic 数据后，调用此方法将数据保存到 Hbase
     *
     * @param records
     * @param ack
     * @param tableName
     * @param family
     */
    static ExecutorService executorService = Executors.newCachedThreadPool();

    public static void save2Hbase(List<ConsumerRecord<?, ?>> records, Acknowledgment ack, String tableName, String family, boolean isAwait) throws Exception {

        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)) {
            throw new Exception("准备插入Hbase时，发现表名或列族为空。");
        }
        logger.warn("批次消费记录数量: recordsSize=" + records.size());
        List<List<Object>> rows = new ArrayList<>();
        Map rowMap = null;

        for (ConsumerRecord<?, ?> record : records) {
            //logger.info("保存表：tableName=" + tableName + ",消费记录：offset =" + record.offset() + ",topic= " + record.topic() + ",partition=" + record.partition() + ",key =" + record.key() + ",value=" + record.value());
            ArrayList<Object> row = new ArrayList<Object>();
            rowMap = JSON.parseObject(record.value().toString(), Map.class);
            row.add(rowMap.remove("rowKey"));
            row.add(rowMap);
            rows.add(row);
        }
        long startTmie = System.currentTimeMillis();
        long writeBufferSize = Long.valueOf(SystemConfig.getProperty("hbase.writeBufferSize", "5242880"));
        try {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(1200);
                        logger.warn("当前线程=" + Thread.currentThread());
                        // HBaseUtil.insertRows(tableName, family, rows, writeBufferSize);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
//            executorService.shutdown();
            if (isAwait) {
                while (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    //logger.warn("still awaiting executorService to be stopped...");
                }
                logger.warn("executorService is stopped");
            }

//            Thread.sleep(1200);
            logger.warn("批次保存时间=" + (System.currentTimeMillis() - startTmie));
        } finally {
            ack.acknowledge();//手动提交偏移量
            logger.warn("complete commit offset");
        }
    }

}
