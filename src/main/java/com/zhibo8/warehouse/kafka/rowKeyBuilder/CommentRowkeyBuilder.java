package com.zhibo8.warehouse.kafka.rowKeyBuilder;

import com.zhibo8.warehouse.entity.Comment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.text.DecimalFormat;

public class CommentRowkeyBuilder {

    /**
     * 生成评论数据的 rowkey
     * @param comment
     * @param regionCode
     * @return
     */
    public static String genRowKey(Comment comment, String regionCode) {
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(comment.getUserId() + "_")
                .append(comment.getCreateTime() + "_")
                .append(comment.getParentId());
        return sb.toString();
    }

    /**
     * 生成评论数据表的 regionCode
     *     1.基本数据：userId+时间（year+month）
     *     2.“用户id + 年月”相同的数据产生的 regionCode 相等
     * @param comment
     * @param regionNum
     * @return
     */
    public static String genRegionCode(Comment comment, int regionNum) {
        //用户id
        String userId = comment.getUserId();
        //String createTime = pingLun.getCreateTime();
        //取出年月
//        String ym = createTime
//                .replaceAll("-", "")
//                .replaceAll(":", "")
//                .replaceAll("/", "")
//                .replaceAll(" ", "")
//                .substring(0, 6);
        //离散1
        byte[] bUserId = Bytes.toBytes(userId);
//        byte[] bTime = Bytes.toBytes(ym);
//        byte[] bAdd = Bytes.add(bUserId, bTime);
//
        String disStr1 = MD5Hash.getMD5AsHex(bUserId)/*.substring(0, 8)*/;
        //离散2
        int disStr2 = disStr1.hashCode();
        //离散数据的绝对值，防止取模为负数
        int regionCode = Math.abs(disStr2) % regionNum;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }
}
