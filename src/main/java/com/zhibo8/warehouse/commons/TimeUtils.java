package com.zhibo8.warehouse.commons;

import java.sql.Date;
import java.text.SimpleDateFormat;

public class TimeUtils {

    public static String timeStemp2DateStr(String ts) {
        Date date = new Date(Long.valueOf(ts));
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String formatTime = df.format(date);
        return formatTime;
    }

}
