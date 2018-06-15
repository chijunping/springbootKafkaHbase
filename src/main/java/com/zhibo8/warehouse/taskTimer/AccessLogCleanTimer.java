package com.zhibo8.warehouse.taskTimer;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

@Component
public class AccessLogCleanTimer {

    Logger logger = LoggerFactory.getLogger(AccessLogCleanTimer.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    @Value("${server.tomcat.basedir}")
    private String basedir;
    @Value("${accesslog.maxHistory}")
    private int maxHistory; //天数
    @Value("${accesslog.maxFileSize}")
    private long maxFileSize; //bytes

    //    每隔 1 小时检查一次
    @Scheduled(fixedDelay = 1000 * 60 * 60)
//    @Scheduled(fixedDelay = 1000 * 5)
    public void cleanTimer() {
        logger.info("开始清理tomcat accessLog,cron : The time is now {}, " + dateFormat.format(new Date()) + "");
        String logPath = basedir + "/logs";
        File fileAccessLog = new File(logPath);
        if (fileAccessLog.isDirectory()) {
            long fileSize = 0;
            // 获取文件夹中的文件集合
            File[] logs = new File(logPath).listFiles();
            Arrays.sort(logs, Collections.reverseOrder());

            // 设置系统这里设置的日期格式,和配置文件里的参数保持一致
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            // 根据 maxHistory 删除
            for (int i = 0; i < logs.length; i++) {
                File log = logs[i];
                // 获取到第i个日志的名称，截取中间的日期字段,转成long型s
                int start = log.getName().indexOf(".") + 1;
                int end = log.getName().lastIndexOf(".");
                // 获取到的日志名称中的时间（2016-12-16）
                String dateStr = log.getName().substring(start, end);
                // 将字符串型的（2016-12-16）转换成long型
                long longDate = 0;
                Date date = null;
                try {
                    date = dateFormat.parse(dateStr);
                    longDate = date.getTime();
                    long currentTimeMillis = System.currentTimeMillis() - longDate;
                    long dateNum = currentTimeMillis / (1000 * 60 * 60 * 24);
                    // 系统时间减去日志名字中获取的时间差大于配置文件中设置的时间删除
                    if (dateNum > maxHistory) {
                        boolean isDelete = log.delete();
                        if (!isDelete) {
                            logger.error("根据时长删除accessLog出错");
                        }
                    }
                } catch (ParseException e) {
                    logger.error("删除accessLog出错", e);
                }
            }
            //根据 maxFileSize 删除
            for (int i = 0; i < logs.length; i++) {
                File log = logs[i];
                if (log.isFile()) {
                    long sizeOfFile = FileUtils.sizeOf(log);
                    fileSize += sizeOfFile;
                    if (fileSize > maxFileSize) {
                        boolean isDelete = log.delete();
                        if (!isDelete) {
                            logger.error("根据大小删除accessLog出错");
                        }
                    }
                }
            }
        }
    }


    /**
     * 每隔 1000 * 3 秒执行一次
     */
    // @Scheduled(fixedDelay = 1000 * 3)
    public void reportCurrentTime() {
        logger.info("The time is now {}", dateFormat.format(new Date()));
    }

    /**
     * 根据cron表达式格式触发定时任务
     * cron表达式格式:
     * 1.Seconds Minutes Hours DayofMonth Month DayofWeek Year
     * 2.Seconds Minutes Hours DayofMonth Month DayofWeek
     * 顺序:
     * 秒（0~59）
     * 分钟（0~59）
     * 小时（0~23）
     * 天（月）（0~31，但是你需要考虑你月的天数）
     * 月（0~11）
     * 天（星期）（1~7 1=SUN 或 SUN，MON，TUE，WED，THU，FRI，SAT）
     * 年份（1970－2099）
     * <p>
     * 注:其中每个元素可以是一个值(如6),一个连续区间(9-12),一个间隔时间(8-18/4)(/表示每隔4小时),一个列表(1,3,5),通配符。
     * 由于"月份中的日期"和"星期中的日期"这两个元素互斥的,必须要对其中一个设置?.
     */
    //@Scheduled(cron = "5 * * * * ?")
    public void cronScheduled() {
        logger.info("cron : The time is now {}", dateFormat.format(new Date()));
    }
}