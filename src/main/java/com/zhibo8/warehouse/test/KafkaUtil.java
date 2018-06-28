package com.zhibo8.warehouse.test;

import com.alibaba.fastjson.JSON;
import com.zhibo8.warehouse.commons.BeanUtil;
import com.zhibo8.warehouse.commons.SecurityUtils;
import com.zhibo8.warehouse.entity.Comment;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.CommentRowkeyBuilder;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.admin.TopicCommand;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class KafkaUtil {
    Logger logger = LoggerFactory.getLogger(KafkaUtil.class);


    String zkHost = "120.55.59.107:2181,121.196.199.76:2181,121.196.218.2:2181/kafka-1.0.0"; //该地址由emr-kafka集群配置而来

    @Test
    public void createKafaTopic() {

        ZkUtils zkUtils = ZkUtils.apply(zkHost, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 创建一个单分区单副本名为t1的topic
        AdminUtils.createTopic(zkUtils, "ad", 24, 2, new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }

    @Test
    public void deleteKafaTopic() {
        ZkUtils zkUtils = ZkUtils.apply(zkHost, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 删除topic 't1'
        AdminUtils.deleteTopic(zkUtils, "ad_dev");
        zkUtils.close();
    }

    @Test
    public void describeKafaTopic() {
        /*ZkUtils zkUtils = ZkUtils.apply(zkHost, 30000, 30000, JaasUtils.isZkSecurityEnabled());
        // 获取topic 'test'的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
        // 查询topic-level属性
        Iterator it = props.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + " = " + value);
        }
        zkUtils.close();*/
        ZkUtils zkUtils = ZkUtils.apply(zkHost, 30000, 30000, JaasUtils.isZkSecurityEnabled());
// 获取topic ‘test‘的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "appLog");
// 查询topic-level属性
        Iterator it = props.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + " = " + value);
        }
        zkUtils.close();
    }

    @Test
    public void alertTopic() {
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
// 增加topic级别属性
        props.put("min.cleanable.dirty.ratio", "0.3");
// 删除topic级别属性
        props.remove("max.message.bytes");
// 修改topic 'test'的属性
        AdminUtils.changeTopicConfig(zkUtils, "test", props);
        zkUtils.close();
    }

    @Test
    public void alertTopic2() {
        String[] options = new String[]{
                "--describe",
                "--zookeeper",
                zkHost,
                "--topic",
                "appLog",
        };
        TopicCommand.main(options);
    }

    @Test
    public void createTopic() {
        String[] options = new String[]{
                "--create",
                "--zookeeper",
                zkHost,
                "--topic",
                "appLog2",
                "--partitions",
                "1",
                "--replication-factor",
                "1"
        };

        //--create --zookeeper CDH01:2181,CDH02:2181,CDH03:2181 --replication-factor 3 --partitions 1 --topic mygirls
        TopicCommand.main(options);
    }

    @Test
    public void desTopic() {
        String[] options = new String[]{
                "--describe",
                "--zookeeper",
                zkHost,
                "--topic",
                "appLog",
        };
        TopicCommand.main(options);
    }


    @Test
    public void testee() {
        String s = UUID.randomUUID().toString();
        System.out.println(s);
    }

    @Test
    public void test() {
        Comment pinglun = new Comment();
        pinglun.setParentId("123");
        pinglun.setUserId("gsdfg");
        pinglun.setCreateTime("2012-12-12 23:23:23");

        pinglun.setUserName("jack");
        pinglun.setMuId("m_21");
        pinglun.setFileName("f_name");
        pinglun.setContent("你好啊");
        pinglun.setUpdateTime("2012-12-12 23:23:23");
        pinglun.setStatus("1");
        pinglun.setUp(".。 ");
        pinglun.setDown(" 。。");
        pinglun.setReport("。。 ");
        pinglun.setDevice(" 。。");
        pinglun.setIp("100.23.2.3.2.3");
        pinglun.setUserInfo("。。 ");
        pinglun.setSysVer(" 。。");
        pinglun.setPlatform(" 。。");
        pinglun.setAppName("。 ");
        pinglun.setAppVer(" 。");
        pinglun.setFigureurl("。。 ");
        pinglun.setLevel("。。 ");
        pinglun.setUVerified(" 。。");
        pinglun.setRoom("。。 ");
        String s = JSON.toJSONString(pinglun);
        System.out.println(s);
    }


    @Test
    public void test001() {
        byte[] bytes1 = Bytes.toBytes(100L);
        byte[] bytes2 = Bytes.toBytes(111L);

        byte[] add = Bytes.add(bytes1, bytes2);
        System.out.println("==" + Bytes.toStringBinary(add));

        ///
        System.out.println("==" + Bytes.toStringBinary(Bytes.toBytes(Long.parseLong(100L + "" + 111L))));

    }

    @Test
    public void testTime() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        long ts = System.currentTimeMillis();
        Date date = new Date(ts);
        String createTime = simpleDateFormat.format(date);
        System.out.println(createTime);
    }

    @Test
    public void regenCode() {
        Comment comment = new Comment();
        comment.setUserId(2 + "");
        String regionCode = CommentRowkeyBuilder.genRegionCode(comment, 50);
        String rowKey = CommentRowkeyBuilder.genRowKey(comment, regionCode);
        System.out.println(rowKey);
    }

    @Test
    public void transBean2Map() throws Exception {
        Comment comment = new Comment();
        comment.setUVerified("123");
        comment.setCreateTime("2013-123-2-3-1");
        comment.setUserId("werwerwerwer");
        Map<String, Object> stringObjectMap = BeanUtil.transBean2Map(comment);
        System.out.println("");
    }

    @Test
    public void time() {
        Date day = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        String formatTime = df.format(day);

        System.out.println(df.format(day));


    }

    @Test
    public void str() {
        String str = "{\\\"os\\\":\\\"android\\\",\\\"os_version\\\":\\\"6.0.1\\\",\\\"version_code\\\":\\\"129\\\",\\\"iemi\\\":\\\"865002036945012\\\",\\\"type\\\":\\\"basketball\\\",\\\"UDID\\\":\\\"865002036945012\\\",\\\"mac\\\":\\\"70:d9:23:c2:27:31\\\",\\\"_platform\\\":\\\"android\\\",\\\"duration\\\":\\\"null\\\",\\\"version_name\\\":\\\"5.0.4\\\",\\\"appname\\\":\\\"zhibo8\\\",\\\"tab\\\":\\\"直播\\\",\\\"visit_team\\\":\\\"骑士\\\",\\\"vType\\\":\\\"action\\\",\\\"_only_care\\\":\\\"1\\\",\\\"imei\\\":\\\"865002036945012\\\",\\\"model\\\":\\\"综合内页\\\",\\\"from\\\":\\\"null\\\",\\\"android_id\\\":\\\"fd67a431df1577fc\\\",\\\"event\\\":\\\"点击视频直播按钮\\\",\\\"home_team\\\":\\\"勇士\\\",\\\"matchid\\\":\\\"124772\\\",\\\"rowKey\\\":\\\"23_865002036945012_20180601103345647\\\"}\n";
        byte[] buf = str.getBytes();
        System.out.println(buf.length + "Byte=" + buf.length / 1024 + "KB");
    }

    @Test
    public void buildRegionCode() {
        for (int i = 0; i < 100000; i++) {
            String regionCode = buildRegionCode(i + "", 50);
            System.out.println("regionCode=" + regionCode);
        }
    }


    private static String buildRegionCode(String imei, int regionNum) {
        //离散1
        byte[] bImei = Bytes.toBytes(imei);
        String disStr1 = MD5Hash.getMD5AsHex(bImei)/*.substring(0, 8)*/;
        //离散2
        int disStr2 = disStr1.hashCode();
        //离散数据的绝对值，防止取模为负数
        int regionCode = Math.abs(disStr2) % regionNum;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }

    @Test
    public void testMap() {
        Map map = new HashMap();
        String model = String.valueOf(map.get("model"));

        System.out.println(model);
        Map mapParams = JSON.parseObject(null, Map.class);
        System.out.println(mapParams);

    }

    @Test
    public void testEncrypt() {
        String encrypt = SecurityUtils.encrypt("{}");
        System.out.println(encrypt);
    }

    @Test
    public void testFile() {
        //如过accessLog 总大小超过 maxFileSize，则保留最近的maxFileSize 范围的文件
        long maxFileSize = 1024 * 100;//110kb
        long maxHistory = 5;
        String logPath = "/app/accessLog/logs";
        File fileAccessLog = new File(logPath);
        if (fileAccessLog.isDirectory()) {
            long totalSize = FileUtils.sizeOfDirectory(fileAccessLog);
            long fileSize = 0;
            System.out.println("Size: " + totalSize + " bytes");

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
                        log.delete();
                        System.out.println(log.getName() + "超过时间，删除..");
                    }
                } catch (ParseException e) {
                    logger.error("删除accessLog出错", e);
                }
            }
            //根据 maxFileSize 删除
            for (int i = 0; i < logs.length; i++) {
                File log = logs[i];
                //删除
                if (log.isFile()) {
                    long sizeOfFile = FileUtils.sizeOf(log);
                    fileSize += sizeOfFile;
                    if (fileSize > maxFileSize) {
                        boolean isDelete = log.delete();
                        if (!isDelete) {
                            logger.error("删除accessLog出错");
                        }
                    }
                }
            }
        }
    }
}
