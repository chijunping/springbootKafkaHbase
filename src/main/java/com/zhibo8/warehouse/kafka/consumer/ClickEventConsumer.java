package com.zhibo8.warehouse.kafka.consumer;

import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.dao.IClickEventDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class ClickEventConsumer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private IClickEventDao clickEventDao;
    @Value("${hbase.table.click}")
    private String clickTableName;

    /**
     * kafka 监听器，批量消费
     *
     * @param records
     * @param ack
     */
    @KafkaListener(topics = "${kafka.consumer.topic-click}", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
        try {
            HbaseMsgSender.save2Hbase(records, ack, clickTableName, Constants.CLICK_FAMILY,true);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
