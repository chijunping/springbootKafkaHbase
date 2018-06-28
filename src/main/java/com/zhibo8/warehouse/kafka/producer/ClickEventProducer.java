package com.zhibo8.warehouse.kafka.producer;

import com.alibaba.fastjson.JSON;
import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.commons.SecurityUtils;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.ClickRowkeyBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 发送 clickEvent 数据到kafka 的 "topic" topic中
 */
@Service
public class ClickEventProducer {
    @Value("${kafka.consumer.topic-click}")
    private String topic_click;

    public Map<String, Object> send(Map<String, Object> paramMap) {
        if (paramMap == null && paramMap.size() == 0) return null;
        Map<String, Object> rsMap = new HashMap<>();
        Map mapV = null;
        // clickEvent->V
        String v = String.valueOf(paramMap.remove("v"));
        //如果v未加密，则不进行解密
        if (v != null && v.startsWith("{") && v.endsWith("}")) {
            mapV = JSON.parseObject(v, Map.class);
        } else {
            String decryptV = SecurityUtils.decrypt(v);
            mapV = JSON.parseObject(decryptV, Map.class);
        }
        if (mapV != null) {
            String event = String.valueOf(mapV.get("event"));
            String model = String.valueOf(mapV.get("model"));
            String vType = String.valueOf(mapV.get("type"));
            // V->params
            String params = String.valueOf(mapV.remove("params"));
            Map mapParams = JSON.parseObject(params, Map.class);
            if (mapParams != null) {
                //安卓的params
                String tab = String.valueOf(mapParams.get("tab"));
                String from = String.valueOf(mapParams.get("from"));
                String visit_team = String.valueOf(mapParams.get("visit_team"));
                String home_team = String.valueOf(mapParams.get("home_team"));
                String type = String.valueOf(mapParams.get("type"));
                String matchid = String.valueOf(mapParams.get("matchid"));
                //ios 的params
                //String tab = String.valueOf(mapParams.get("tab"));
                //String from = String.valueOf(mapParams.get("from"));
                String duration = String.valueOf(mapParams.get("duration"));

                //将解析出的字段封装到一个Map 里
                paramMap.put("event", event);
                paramMap.put("model", model);
                paramMap.put("vType", vType);

                paramMap.put("tab", tab);
                paramMap.put("from", from);
                paramMap.put("visit_team", visit_team);
                paramMap.put("home_team", home_team);
                paramMap.put("type", type);
                paramMap.put("matchid", matchid);
                paramMap.put("duration", duration);
            }
        }
        //计算regionCode 用 imei 作为用户唯一标识，计算region 编号
        Date day = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String formatTime = df.format(day);
        String UDID = String.valueOf(paramMap.get("UDID")); //设备id
        String regionCode = ClickRowkeyBuilder.buildRegionCode(UDID, Constants.CLICK_REGINNUM);
        //计算 rowkey
        String rowKey = ClickRowkeyBuilder.buildRowKey(UDID, formatTime, regionCode);
        //将 rowkey 放到messageMap中
        paramMap.put("rowKey", rowKey);
        boolean idSended = CommonProducer.send(topic_click, paramMap);
        rsMap.put("idSended", idSended);
        rsMap.put("rowKey", rowKey);
        return rsMap;
    }
}
