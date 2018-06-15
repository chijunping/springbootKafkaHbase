package com.zhibo8.warehouse.kafka.rowKeyBuilder;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.text.DecimalFormat;

/**
 * 通用 regionCode 生成者，只根据 id 和 regionNum 生成 regionCode
 */
public class CommonRowkeyBuilder {

    /**
     * 通用 rowkey 生成方法
     *
     * @param id
     * @param regionCode
     * @return
     */
    public static String buildRowKey(String id,String time, String regionCode) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(id+ "_")
                .append(time);
        return sb.toString();
    }

    /**
     * 通用 regionCode 生成方法，只根据 id 和 regionNum 生成 regionCode
     *
     * @param id
     * @param regionNum
     * @return
     */
    public static String buildRegionCode(String id, int regionNum) {
        //离散1
        byte[] bImei = Bytes.toBytes(id);
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
