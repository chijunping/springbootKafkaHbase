package com.zhibo8.warehouse;

import com.zhibo8.warehouse.commons.BeanUtil;
import com.zhibo8.warehouse.commons.HBaseUtil;
import com.zhibo8.warehouse.dao.ICommentDao;
import com.zhibo8.warehouse.entity.Comment;
import com.zhibo8.warehouse.kafka.producer.CommentProducer;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.CommentRowkeyBuilder;
import com.zhibo8.warehouse.service.impl.PingLunServiceImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootApplicationTest {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private PingLunServiceImpl pingLunService;
    @Autowired
    private ICommentDao ICommentDao;
    private String hbase_zookeeper_quorum ="hb-proxy-xxxxxx-002.hbase.rds.aliyuncs.com:2181,hb-proxy-xxxxxx-001.hbase.rds.aliyuncs.com:2181,hb-proxy-xxxxxx-003.hbase.rds.aliyuncs.com:2181";
    private int initialSize = 1;
    private int minIdle = 5;
    private int maxActive = 5;

    private void initHbase() {
        String zkAddress = hbase_zookeeper_quorum;
        HBaseUtil.init(1, 1, 2000L, zkAddress);
    }

    @Autowired
    private CommentProducer commentProducer;

    @Test
    public void testInsert() {
        initHbase();
        //创建请求数据
        for (int i = 0; i <= 100000; i++) {
            Comment comment = new Comment();
            //时间转换
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
            long ts = System.currentTimeMillis();
            Date date = new Date(ts);
            String createTime = simpleDateFormat.format(date);
            comment.setCreateTime(createTime);
            comment.setUserId(i + "");
            //准备插入数据
            String regionCode = CommentRowkeyBuilder.genRegionCode(comment, 50);
            String rowKey = CommentRowkeyBuilder.genRowKey(comment, regionCode);
            comment.setRowKey(rowKey);
            Map<String, Object> paramMap = BeanUtil.transBean2Map(comment);
            //往 kafka 生产数据
            boolean isSended = commentProducer.send(paramMap);
        }
//        Map<String, Object> rsMap = new HashMap<>();
//        rsMap.put("isSended", isSended);
//        return rsMap;
    }


}
