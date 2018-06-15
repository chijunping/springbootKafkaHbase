package com.zhibo8.warehouse.controller;

import com.zhibo8.warehouse.kafka.producer.ClickEventProducer;
import com.zhibo8.warehouse.service.IClickEventService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 点击流事件
 */
@RestController
public class ClickEventController {


    @Autowired
    private ClickEventProducer clickEventProducer;
    @Autowired
    private IClickEventService clickEventService;

    //https://tongji.zhibo8.cc/api/statis/add
    @RequestMapping("/api/statis/add")
    public Map<String, Object> add(
            String device,
            String _only_care,
            String version_code,
            String os,
            String UDID, //苹果设备id
            String udid, //安卓设备id
            String os_version,
            String v,
            String appname,
            String _platform,
            String IDFA,
            String mac,
            String iemi, //安卓设备id
            String version_name,
            String imei,
            String android_id
    ) {
        Map<String, Object> paramMap = new HashMap<>();
        //封装 android 请求参数
        paramMap.put("version_code", version_code);
        paramMap.put("_platform", _platform);
        paramMap.put("v", v);
        paramMap.put("_only_care", _only_care);
        paramMap.put("os", os);
        paramMap.put("os_version", os_version);
        paramMap.put("appname", appname);
        if (_platform.contains("android")) {
            paramMap.put("mac", mac);
            paramMap.put("iemi", iemi);
            paramMap.put("version_name", version_name);
            paramMap.put("imei", imei);
            paramMap.put("UDID", udid);//新增 UDID 参数，作设备唯一标识
            paramMap.put("android_id", android_id);
        } else {
            paramMap.put("device", device);
            paramMap.put("UDID", UDID);
            paramMap.put("IDFA", IDFA);
        }
        return clickEventProducer.send(paramMap);
    }

    @RequestMapping("/getClickEventsByUDID")
    public List<Map<String, Object>> getClickEventsByUDID(String udid) {
        List<Map<String, Object>> clickEventList = clickEventService.getClickEventsByUDID(udid);
        return clickEventList;
    }

    @RequestMapping("/getClickEventsByRowkeys")
    public List<Map<String, Object>> getClickEventsByRowkeys(@RequestBody String[] rowkeys) {
        List<Map<String, Object>> clickEventList = clickEventService.getClickEventsByRowkeys(Arrays.asList(rowkeys));
        return clickEventList;
    }

}
