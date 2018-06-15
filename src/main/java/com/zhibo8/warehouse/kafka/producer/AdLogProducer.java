package com.zhibo8.warehouse.kafka.producer;

import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.commons.TimeUtils;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.ClickRowkeyBuilder;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.CommonRowkeyBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class AdLogProducer {
    @Value("${kafka.consumer.topic-ad}")
    private String topic_ad;

    public Map<String, Object> send(Map<String, Object> paramMap) {
        String udid = String.valueOf(paramMap.get("udid"));
        String regionCode = CommonRowkeyBuilder.buildRegionCode(udid, Constants.AD_REGINNUM);
        //计算 rowkey
        String time = TimeUtils.timeStemp2DateStr(String.valueOf(paramMap.get("ts")));
        String rowKey = ClickRowkeyBuilder.buildRowKey(udid,time, regionCode);
        //将 rowkey 放到messageMap中
        paramMap.put("rowKey", rowKey);
        boolean idSended = CommonProducer.send(topic_ad, paramMap);
        Map<String, Object> rsMap = new HashMap<>();
        rsMap.put("idSended", idSended);
        rsMap.put("rowKey", rowKey);
        return rsMap;
    }
}
