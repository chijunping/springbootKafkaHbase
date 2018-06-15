package com.zhibo8.warehouse.kafka.producer;

import com.zhibo8.warehouse.commons.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * 发送 comment 数据到kafka 的 "comment" topic中
 */
@Service
public class CommentProducer {
    @Value("${kafka.consumer.topic-comment}")
    private String topic_comment;

    public boolean send(Map<String, Object> messageMap) {
        /*//KafkaProducerPoolInitial.send(Constants.COMSUMER_TOPIC, JSON.toJSON(messageMap).toString());
        //首条数据不会为addcallback()
        ListenableFuture future = template.send(Constants.COMSUMER_TOPIC, JSON.toJSON(messageMap).toString());
        future.addCallback(ok -> {
            logger.info("消息发送成功：" + messageMap);
            this.isSended = true;
        }, throwable -> {
            logger.info("消息发送失败：" + messageMap);
            this.isSended = false;
        });
        return isSended;*/

        return CommonProducer.send(topic_comment, messageMap);
    }
}
