package com.zhibo8.warehouse.entity;


import lombok.Data;

/**
 * 评论 Bean
 */
@Data
public class Comment {
    private String rowKey;
    private String id; //记录id
    private String userName;
    private String userId;
    private String parentId;
    private String muId;
    private String fileName;
    private String content;
    private String createTime;
    private String updateTime;
    private String status;
    private String up;
    private String down;
    private String report;
    private String device;
    private String ip;
    private String userInfo;
    private String sysVer;
    private String platform;
    private String appName;
    private String appVer;
    private String figureurl;
    private String level;
    private String uVerified;
    private String room;

}
