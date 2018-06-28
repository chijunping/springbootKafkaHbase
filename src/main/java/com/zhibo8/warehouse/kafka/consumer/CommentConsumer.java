package com.zhibo8.warehouse.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.zhibo8.warehouse.commons.BeanUtil;
import com.zhibo8.warehouse.commons.Constants;
import com.zhibo8.warehouse.dao.ICommentDao;
import com.zhibo8.warehouse.entity.Comment;
import com.zhibo8.warehouse.kafka.rowKeyBuilder.CommentRowkeyBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;


@Component
public class CommentConsumer {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ICommentDao commentDao;

    /**
     * kafka 监听器，批量消费
     *
     * @param records
     * @param ack
     */
//    @KafkaListener(id = Constants.COMSUMER_GROUPID, topics = {Constants.COMMENT_TOPIC}, containerFactory = "batchFactory")
    @KafkaListener(/*id = "${kafka.consumer.groupId}", */topics ="${kafka.consumer.topic-comment}", containerFactory = "batchFactory")
    public void listen(List<ConsumerRecord<?, ?>> records, Acknowledgment ack) {
        logger.info("批次消费记录数量: recordsSize=" + records.size());
        List<List<Object>> rows = new ArrayList<>();
        Comment comment = null;
        try {
            for (ConsumerRecord<?, ?> record : records) {
                logger.info("消费记录：offset =" + record.offset() + ",topic= " + record.topic() + ",partition=" + record.partition() + ",key =" + record.key() + ",value=" + record.value());
                ArrayList<Object> row = new ArrayList<Object>();
                comment = JSON.parseObject(record.value().toString(), Comment.class);
                row.add(comment.getRowKey());
                row.add(BeanUtil.transBean2Map(comment));
                rows.add(row);
            }
            commentDao.insertRows(Constants.COMMENT_TABLE_NAME, Constants.COMMENT_FAMILY, rows);
        } catch (Exception e) {
            logger.error("消费kafka 往Hbase写入时异常: ",e);
        } finally {
            ack.acknowledge();//手动提交偏移量
            logger.info("complete commit offset");
        }
    }

//    @Value("${kafka.consumer.groupId-comment}")
//    private String groupId;
    /**
     * kafka 监听器，逐条消费
     */

	/*@KafkaListener(topics = {"appLog"})
	public void listen(ConsumerRecord<?, ?> record) {
		logger.info("Kafka2HbaseConsumer 消费者监听器：\n"+ JSON.toJSONString(record.value()));
		Map<String,Object> paramMap = JSON.parseObject(record.value().toString(), Map.class);
		String rowkey = "pl_"+(String) paramMap.get("articleId")+"_"+System.currentTimeMillis();
		boolean b = pingLunDao.insertRow(Constants.COMMENT_TABLE_NAME, rowkey, Constants.COMMENT_FAMILY, paramMap);
		System.out.println(b);
	}*/


}