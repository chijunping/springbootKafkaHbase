package com.zhibo8.warehouse.commons;


/***
 * 程序中使用的静态字段定义在此处
 */

public class Constants {
	/** Hbase 命名空间*/
	public static final String NS_BIGDATA="bigdata";//
	public static final String NS_AUTHOR="zhibo8";//


	/** 评论信息表**/
	public static final String COMMENT_TABLE_NAME="bigdata:comment";//表名
	public static final String COMMENT_FAMILY="pl";//列族1，评论
	public static final int COMMENT_REGIONNUM=50;//comment 表的 region 数

	public static final String COMMENT_USERID="userId";//列，评论者id
	public static final String COMMENT_PARENTID="parentId";//列：回复对象id
	public static final String COMMENT_CONTENT="content";//列：评论文本
	public static final String COMMENT_CREATETIME="createTime";//列：评论时间
	public static final String COMMENT_STATUS="status";//列：评论的状态
	/** 点击事件表**/
	//public static final String CLICK_TABLENAME="bigdata:click";//表名
	public static final String CLICK_FAMILY="info";//列族
	public static final int CLICK_REGINNUM=50;//click 表的 region 数
	public static final int CLICK_VERSIONNUM=1;//数据版本数
	/** 广告日志表**/
	public static final String AD_FAMILY="info";//列族
	public static final int AD_REGINNUM=50;//click 表的 region 数
	public static final int AD_VERSIONNUM=1;//数据版本数

	/** 二级索引表*/
	public static final String INDEX_TABLENAME="bigdata:index";// 索引表 表名
	public static final String INDEX_FAMILY="idf";// 列族
	public static final String INDEX_COMMENT_PREFX="pl_";// 评论数据的索引前缀
	public static final String INDEX_CHIDREN_ROWKEYS="chidrenRowkeys";// 子id 列


	/**分隔符*/
//	public static final String STRING_SEPARATOR = "|" ;
//	public static final String ROWKEY_SEPARATOR = "_" ;//rowkey的分隔符

	/** kafka */
//	public static final String COMMENT_TOPIC="comment";
//	public static final String CLICK_TOPIC="click";
//	public static final String COMSUMER_GROUPID="group-warehouse";

}
