package com.zhibo8.warehouse.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.zhibo8.warehouse.commons.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
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

@Component
public class HbaseMsgSender {
    private static Logger log = LoggerFactory.getLogger(HbaseMsgSender.class);

    private static Environment env;


    /**
     * 注入静态变量
     * 注意：1、通过setter 方法注入
     *       2、当前需要注入静态变量的类，需要加入IOC 容器，（需要加上@Component 注解）
     * @param env_
     */
    @Autowired
    public void setEnvironment(Environment env_) {
        HbaseMsgSender.env = env_;
    }

    /**
     * 批量消费 kafka 的topic 数据后，调用此方法将数据保存到 Hbase
     *
     * @param records
     * @param ack
     * @param tableName
     * @param family
     */
    public static void save2Hbase(List<ConsumerRecord<?, ?>> records, Acknowledgment ack, String tableName, String family) {
        log.info("批次消费记录数量: recordsSize=" + records.size());
        List<List<Object>> rows = new ArrayList<>();
        Map rowMap = null;
        try {
            for (ConsumerRecord<?, ?> record : records) {
                log.info("保存表：tableName=" + tableName + ",消费记录：offset =" + record.offset() + ",topic= " + record.topic() + ",partition=" + record.partition() + ",key =" + record.key() + ",value=" + record.value());
                ArrayList<Object> row = new ArrayList<Object>();
                rowMap = JSON.parseObject(record.value().toString(), Map.class);
                row.add(rowMap.remove("rowKey"));
                row.add(rowMap);
                rows.add(row);
            }
            long writeBufferSize = Long.valueOf(env.getProperty("hbase.writeBufferSize"));
            boolean isSaved = HBaseUtil.insertRows(tableName, family, rows, writeBufferSize);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("消费kafka 往Hbase写入时异常：\n" + e.getMessage());
        } finally {
            ack.acknowledge();//手动提交偏移量
            log.info("complete commit offset");
        }
    }
}
