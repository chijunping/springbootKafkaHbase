package com.zhibo8.warehouse.controller;

import com.zhibo8.warehouse.kafka.producer.AdLogProducer;
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
public class AdLogController {


    @Autowired
    private AdLogProducer adLogProducer;
    @Autowired
    private IClickEventService clickEventService;

    //andriod: http://118.178.168.156:8091/allOne.php?ad_name=news_list&_only_care=3
    //ios: http://118.178.168.156:8091/allOne.php?ad_name=news_list_ios&_only_care=3&pk=
    @RequestMapping("/allOne.php")
    public Map<String, Object> add(
            //andriod
            String ad_name,//
            String _only_care,//
            String osv,//
            String ssid,
            String isboot, //
            String geo, //
            String iem,
            String devicetype,//
            String version_name,
            String pkgname,
            String ts,//
            String category,
            String imei,
            String label,//
            String adid,
            String type,
            String orientation,//
            String android_id,
            String lan,//
            String tag,//
            String appname,//
            String csinfo,
            String mac,
            String version_code,//
            String dvw,//
            String dvh,//
            String net,//
            String density,//
            String os,//
            String platform,//
            String operator,//
            String ip,//
            String vendor,//
            String model,//
            // ios
            String pk,
            String device,
            String adh,
            String openudid,
            String adw,
            String idfa
    ) {
        Map<String, Object> paramMap = new HashMap<>();
        //封装 android 请求参数
        paramMap.put("ad_name", ad_name);
        paramMap.put("_only_care", _only_care);
        paramMap.put("osv", osv);
        paramMap.put("isboot", isboot);
        paramMap.put("geo", geo);//
        paramMap.put("devicetype", devicetype);
        paramMap.put("ts", ts);
        paramMap.put("label", label);
        paramMap.put("orientation", orientation);
        paramMap.put("lan", lan);
        paramMap.put("tag", tag);
        paramMap.put("appname", appname);
        paramMap.put("version_code", version_code);
        paramMap.put("dvw", dvw);
        paramMap.put("dvh", dvh);
        paramMap.put("net", net);
        paramMap.put("density", density);
        paramMap.put("os", os);
        paramMap.put("platform", platform);
        paramMap.put("operator", operator);
        paramMap.put("ip", ip);
        paramMap.put("vendor", vendor);
        paramMap.put("model", model);
        if (platform.contains("android")) {
            paramMap.put("ssid", ssid);
            paramMap.put("iem", iem);
            paramMap.put("version_name", version_name);
            paramMap.put("pkgname", pkgname);
            paramMap.put("category", category);
            paramMap.put("adid", adid);
            paramMap.put("type", type);
            paramMap.put("android_id", android_id);
            paramMap.put("csinfo", csinfo);
            paramMap.put("mac", mac);
            paramMap.put("imei", imei);
            paramMap.put("udid", imei);
        } else {
            paramMap.put("pk", pk);
            paramMap.put("device", device);
            paramMap.put("adh", adh);
            paramMap.put("adw", adw);
            paramMap.put("idfa", idfa);
            paramMap.put("openudid", openudid);
            paramMap.put("udid", openudid);
        }
        return adLogProducer.send(paramMap);
    }

    @RequestMapping("/getClickEventsByUDID1")
    public List<Map<String, Object>> getClickEventsByUDID(String udid) {
        List<Map<String, Object>> clickEventList = clickEventService.getClickEventsByUDID(udid);
        return clickEventList;
    }

    @RequestMapping("/getClickEventsByRowkeys1")
    public List<Map<String, Object>> getClickEventsByRowkeys(@RequestBody String[] rowkeys) {
        List<Map<String, Object>> clickEventList = clickEventService.getClickEventsByRowkeys(Arrays.asList(rowkeys));
        return clickEventList;
    }

}
