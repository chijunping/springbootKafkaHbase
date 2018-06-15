package com.zhibo8.warehouse.controller;

import com.zhibo8.warehouse.commons.BeanUtil;
import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.entity.Comment;
import com.zhibo8.warehouse.kafka.producer.CommentProducer;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.CommentRowkeyBuilder;
import com.zhibo8.warehouse.service.PingLunService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.*;


@RestController
@RequestMapping("/pinglun")
public class CommentController {
    private static final String TABLE_NAME = "comment";
    private static final String FAMILY = "pl";//评论 pinglun

    @Autowired
    private PingLunService pingLunService;
    @Autowired
    private Environment environment;
    @Value("${hbase.pool.maxWait}")
    private String maxWait;


    /**
     * 增加一条评论，请求中添加该评论的某一项属性
     *
     * @param paramMap
     * @return
     */
    @RequestMapping("/insertCell")
    public Map insertCell(@RequestBody Map<String, Object> paramMap) {
        String rowKey = "pl_1231233421_" + System.currentTimeMillis();
        String quailifer = "content";
        String value = "你好，单个cell插入测试！";
        boolean isInserted = pingLunService.insertCell(TABLE_NAME, rowKey, FAMILY, quailifer, value);
        Map<String, Object> rsMap = new HashMap<>();
        if (isInserted) {
            rsMap.put("rowKey", rowKey);
            rsMap.put("content", value);
            rsMap.put("msg", "插入成功@！");
        } else {
            rsMap.put("msg", "插入失败！");
        }
        return rsMap;
    }


    /**
     * 增加一条评论，请求中添加该评论的属性
     *
     * @param paramMap
     * @return
     */
    @RequestMapping("/insertRow")
    public Map insertRow(@RequestBody Map<String, Object> paramMap) {
        boolean isSended = commentProducer.send(paramMap);
        Map<String, Object> rsMap = new HashMap<>();
        rsMap.put("isSended", isSended);
        return rsMap;
    }


    @Autowired
    private CommentProducer commentProducer;


    @RequestMapping("/testInsert")
    public Map<String, Object> testInsert() {
        //创建请求数据
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        long ts = System.currentTimeMillis();
        Date date = new Date(ts);
        String createTime = simpleDateFormat.format(date);
        Comment comment = new Comment();
        comment.setUserId("u_1" + UUID.randomUUID());
        comment.setCreateTime(createTime);
        comment.setParentId("u_2" + UUID.randomUUID());
        //准备插入数据
        Map<String, Object> paramMap = BeanUtil.transBean2Map(comment);
        boolean isSended = commentProducer.send(paramMap);
        Map<String, Object> rsMap = new HashMap<>();
        rsMap.put("isSended", isSended);
        return rsMap;
    }

    @PostMapping("/insertComment")
    public Map<String, Object> insertComment(@RequestBody Comment comment) {
        //时间转换
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        long ts = System.currentTimeMillis();
        Date date = new Date(ts);
        String createTime = simpleDateFormat.format(date);
        comment.setCreateTime(createTime);
        //准备插入数据
        String regionCode = CommentRowkeyBuilder.genRegionCode(comment, Constants.COMMENT_REGIONNUM);
        String rowKey = CommentRowkeyBuilder.genRowKey(comment, regionCode);
        comment.setRowKey(rowKey);
        Map<String, Object> paramMap = BeanUtil.transBean2Map(comment);
        //往 kafka 生产数据
        boolean isSended = commentProducer.send(paramMap);
        Map<String, Object> rsMap = new HashMap<>();
       // rsMap.put("isSended", isSended);
        rsMap.put("rowKey", rowKey);
        return rsMap;
    }

    @RequestMapping("/getRowsCountByUserId")
    public String getRowsCountByUserId(String userId) {
        List<Map<String, Object>> commentsList = pingLunService.getRowsByArticleId(userId);
        return commentsList.size() + "";
    }

    /**
     * 获取指定用户的所有评论
     *
     * @param userId
     * @return
     */
    @RequestMapping("/getCommentsByUserId")
    public List<Map<String, Object>> getCommentsByUserId(String userId) {

        List<Map<String, Object>> commentsList = pingLunService.getRowsByArticleId(userId);
        return commentsList;
    }

}