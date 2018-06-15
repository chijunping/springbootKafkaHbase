package com.zhibo8.warehouse.test;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author rjsong
 */
public class TestKafka {

    public static void main(String[] args){
        String zkHost = "120.55.59.107:2181,121.196.199.76:2181,121.196.218.2:2181/kafka-1.0.0"; //该地址由emr-kafka集群配置而来

        ZkUtils zkUtils = ZkUtils.apply(zkHost,30000,30000,
                JaasUtils.isZkSecurityEnabled());

        //创建一个单分区副本名为test2的topic
        AdminUtils.createTopic(zkUtils,"test2",1,1,new Properties(),
                RackAwareMode.Enforced$.MODULE$);
        System.out.println("创建成功");

        //查询topic
        Properties props = AdminUtils.fetchEntityConfig(zkUtils,
                ConfigType.Topic(),"test2");
        //查询topic-level的属性
        Iterator iterator =props.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry entry = (Map.Entry)iterator.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key+" = "+value);
        }
        System.out.println("success.");
        zkUtils.close();

        //修改topic
       /* Properties props = AdminUtils.fetchEntityConfig(zkUtils,
                ConfigType.Topic(),"test2");
        //增加topic级别属性
        props.put("min.cleanable.dirty.ratio","0.3");
        AdminUtils.changeTopicConfig(zkUtils,"test2",props);
        System.out.println("修改成功");*/

       //删除topic
//        AdminUtils.deleteTopic(zkUtils,"test2");
//        System.out.println("删除成功");
        zkUtils.close();
    }
}

