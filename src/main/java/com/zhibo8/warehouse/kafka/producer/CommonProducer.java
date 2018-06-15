package com.zhibo8.warehouse.kafka.producer;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Map;

@Component
public class CommonProducer {
    private static final Logger logger = LoggerFactory.getLogger(CommonProducer.class);

    //kafka send() 方法回掉函数标识消息生产的成功与否
    private static boolean isSended = true;
    //@Autowired 静态变量属于类，不是对象，无法直接注入，利用setter方法注入
    private static KafkaTemplate template;

    @Autowired//利用setter方法注入 template 实例对象
    public void setKafkaTemplate(KafkaTemplate template) {
        CommonProducer.template = template;
    }


    public static boolean send(String topic, Map<String, Object> messageMap) {
        //KafkaProducerPoolInitial.send(Constants.COMSUMER_TOPIC, JSON.toJSON(messageMap).toString());
        //首条数据不会为addcallback()
        ListenableFuture future = template.send(topic, JSON.toJSON(messageMap).toString());
        future.addCallback(ok -> {
            logger.info("消息发送成功：" + messageMap);
            isSended = true;
        }, throwable -> {
            logger.info("消息发送失败：" + messageMap);
            isSended = false;
        });
        return isSended;
    }
}
