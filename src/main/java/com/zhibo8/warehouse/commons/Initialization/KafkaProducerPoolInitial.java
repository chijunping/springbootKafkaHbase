package com.zhibo8.warehouse.commons.Initialization;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * 测试，没用在项目中
 */
@Component
public class KafkaProducerPoolInitial /*implements ApplicationContextAware */{

    @Autowired
    KafkaTemplate template;
    private static KafkaTemplate[] pool;


    private static int threadNum=10;
    private static int index = 0; // 轮循id

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerPoolInitial.class);

   // @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        logger.info("Init DCKafkaProducerPool: threadNum=" + threadNum);
        pool = new KafkaTemplate[threadNum];
        for (int i = 0; i < threadNum; i++) {
            pool[i] = ctx.getBean(KafkaTemplate.class);
        }
    }

    public static void send(String topic, String messageMap) {
        int i = index++ % threadNum;
        KafkaTemplate template = pool[i];
        System.out.println("template实例hashCode: "+template.hashCode());
        ListenableFuture future = template.send(topic, messageMap);
        future.addCallback(o -> System.out.println("send-消息发送成功：" + messageMap), throwable -> System.out.println("消息发送失败：" + messageMap));
    }
}