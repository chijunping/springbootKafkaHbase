package com.zhibo8.warehouse.kafka.rowKeyBuilder;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.text.DecimalFormat;

public class ClickRowkeyBuilder {

    /**
     * 生成点击事件数据的 rowkey
     *
     * @param imei
     * @param regionCode
     * @return
     */
    public static String buildRowKey(String imei,String time, String regionCode) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(imei+ "_")
                .append(time);
        return sb.toString();
    }

    /**
     * 生成点击数据表的 regionCode
     * 1.基本数据：userId+时间（year+month）
     * 2.“用户id + 年月”相同的数据产生的 regionCode 相等
     *
     * @param imei
     * @param regionNum
     * @return
     */
    public static String buildRegionCode(String imei, int regionNum) {
        //离散1
        byte[] bImei = Bytes.toBytes(imei);
//        byte[] bTime = Bytes.toBytes(ym);
//        byte[] bAdd = Bytes.add(bUserId, bTime);
//
        String disStr1 = MD5Hash.getMD5AsHex(bImei)/*.substring(0, 8)*/;
        //离散2
        int disStr2 = disStr1.hashCode();
        //离散数据的绝对值，防止取模为负数
        int regionCode = Math.abs(disStr2) % regionNum;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }
}
