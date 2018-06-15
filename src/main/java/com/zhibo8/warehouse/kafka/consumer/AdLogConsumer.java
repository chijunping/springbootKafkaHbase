package com.zhibo8.warehouse.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.IAdLogDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Component
public class AdLogConsumer {
    private Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private IAdLogDao adLogDao;
    @Value("${hbase.table.ad}")
    private String table_ad;


    /**
     * kafka 监听器，批量消费
     *
     * @param records
     * @param ack
     */
    @KafkaListener(topics = "${kafka.consumer.topic-ad}", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
        HbaseMsgSender.save2Hbase(records, ack, table_ad, Constants.AD_FAMILY);
    }
}
