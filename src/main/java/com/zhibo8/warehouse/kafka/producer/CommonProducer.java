package com.zhibo8.warehouse.kafka.producer;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;

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
        //发送数据
        ListenableFuture listenableFuture = template.send(topic, JSON.toJSON(messageMap).toString());
        //发送成功后回调
        SuccessCallback successCallback = new SuccessCallback() {
            @Override
            public void onSuccess(Object result) {
                logger.info("消息发送成功：" + messageMap);
                isSended = true;
            }
        };

        //发送失败回调
        FailureCallback failureCallback = new FailureCallback() {
            @Override
            public void onFailure(Throwable e) {
                logger.info("消息发送失败：" + messageMap);
                logger.error("消息发送失败：" + messageMap + "\n" + e.getMessage(), e);
                isSended = false;
            }
        };

        listenableFuture.addCallback(successCallback, failureCallback);
        return isSended;
    }
}
