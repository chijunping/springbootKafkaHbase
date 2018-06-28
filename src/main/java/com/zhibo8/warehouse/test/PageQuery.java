package com.zhibo8.warehouse.test;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


public class PageQuery {

    private final Logger logger = LoggerFactory.getLogger(PageQuery.class);
    private static Configuration conf = null;
    private static Connection conn = null;
    private static HTable table = null;
    private static TableName tn = null;
    private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
    private static final String ZK_CONNECT_VALUE = "hb-proxy-pub-bp1987l1fy04etj46-002.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-001.hbase.rds.aliyuncs.com:2181,hb-proxy-pub-bp1987l1fy04etj46-003.hbase.rds.aliyuncs.com:2181";

    static {
        try {
            conf = HBaseConfiguration.create();
            conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
            conn = ConnectionFactory.createConnection(conf);
            tn = TableName.valueOf("bigdata:comment");
            table = (HTable) conn.getTable(tn);
        } catch (Exception e) {
            System.err.println("初始化参数失败");
        }
    }

    @Test
    public void testt() throws Exception {
        PageData dataMap = getDataMap("bigdata:comment", "00_10152_20180522092417684_null", "49_10174_20180522092417685_null", null, 2, 10);
        System.out.println(dataMap);
    }

    /**
     * 数据查询代码
     *
     * @param tableName   表名
     * @param startRow    起点key
     * @param stopRow     结束key
     * @param objKey      筛选id
     *                    数据间隔(每几个数据取一个)
     * @param currentPage 当前页
     * @param pageSize    每页数量
     * @return
     * @throws IOException
     */


    public PageData getDataMap(String tableName, String startRow, String stopRow, String objKey, Integer currentPage, Integer pageSize) throws Exception {

        List<Map<String, String>> mapList = null;
        mapList = new LinkedList<Map<String, String>>();

        ResultScanner scanner = null;
        // 为分页创建的封装类对象，下面有给出具体属性  
        PageData tbData = null;
        try {

            // 计算起始页和结束页  
            Integer firstPage = (currentPage - 1) * pageSize;

            Integer endPage = firstPage + pageSize;

            // 从表池中取出HBASE表对象  
            HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
            // 获取筛选对象
            Scan scan = getScan(startRow, stopRow);
            // 给筛选对象放入过滤器(true标识分页,具体方法在下面)  
            // scan.setFilter(packageFilters(true));  
            // ---------------添加过滤查询  
            // if (!StringUtils.isBlank(objKey)) {  
            // FilterList filterList = new FilterList();  
            // List<String> arr = new ArrayList<String>();  
            // arr.add("info,tag, " + objKey);  
            // for (String v : arr) { //  
            // String[] s = v.split(",");  
            // filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes(s[0]), Bytes.toBytes(s[1]), CompareOp.EQUAL, Bytes.toBytes(s[2])));  
            // scan.setFilter(filterList);  
            // }  
            // }  
            if (!StringUtils.isBlank(objKey)) {// key最后的值=objKey  
                Filter filterList = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(objKey));
                scan.setFilter(filterList);
            }

            // ----------------添加过滤查询  
            // 缓存 10000 条数据
            scan.setCaching(10000);
            scan.setCacheBlocks(false);
            scanner = table.getScanner(scan);
            int i = 0;
            List<byte[]> rowList = new LinkedList<byte[]>();
            // 遍历扫描器对象， 并将需要查询出来的数据row key取出  
            for (Result result : scanner) {
                String row = toStr(result.getRow());
                if (i >= firstPage && i < endPage) {
                    System.out.println(row);
                    rowList.add(getBytes(row));
                }
                i++;
            }

            // 获取取出的row key的GET对象  
            List<Get> getList = getList(rowList);
            Result[] results = table.get(getList);
            // 遍历结果  
            for (Result result : results) {
                Map<byte[], byte[]> fmap = packFamilyMap(result);
                Map<String, String> rmap = packRowMap(fmap, result);
                mapList.add(rmap);
            }

            // 封装分页对象  
            tbData = new PageData();
            tbData.setCurrentPage(currentPage);
            tbData.setPageSize(pageSize);
            tbData.setTotalCount(i);
            tbData.setTotalPage(getTotalPage(pageSize, i));
            tbData.setResultList(mapList);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            closeScanner(scanner);
        }

        return tbData;

    }

    private static int getTotalPage(int pageSize, int totalCount) {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            return n;
        } else {
            return ((int) n) + 1;
        }

    }

    // 获取扫描器对象  
    private static Scan getScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.setStartRow(getBytes(startRow));
        scan.setStopRow(getBytes(stopRow));

        return scan;
    }

    /**
     * 封装查询条件
     */
    private static FilterList packageFilters(boolean isPage) {
        FilterList filterList = null;
        // MUST_PASS_ALL(条件 AND) MUST_PASS_ONE（条件OR）  
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = null;
        Filter filter2 = null;
        filter1 = newFilter(getBytes("family1"), getBytes("column1"), CompareOp.EQUAL, getBytes("condition1"));
        filter2 = newFilter(getBytes("family2"), getBytes("column1"), CompareOp.LESS, getBytes("condition2"));
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        if (isPage) {
            //????
            filterList.addFilter(new FirstKeyOnlyFilter());
        }
        return filterList;
    }

    private static Filter newFilter(byte[] f, byte[] c, CompareOp op, byte[] v) {
        return new SingleColumnValueFilter(f, c, op, v);
    }

    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }

    /**
     * 封装每行数据
     *
     * @param result
     */
    private static Map<String, String> packRowMap(Map<byte[], byte[]> dataMap, Result result) {
        Map<String, String> map = new LinkedHashMap<String, String>();

        for (byte[] key : dataMap.keySet()) {

            byte[] value = dataMap.get(key);

            map.put(toStr(key), toStr(value));

        }
        map.put("key", Bytes.toString(result.getRow()));
        return map;
    }

    /* 根据ROW KEY集合获取GET对象集合 */
    private static List<Get> getList(List<byte[]> rowList) {
        List<Get> list = new LinkedList<Get>();
        for (byte[] row : rowList) {
            Get get = new Get(row);
            get.addColumn(getBytes("pl"), getBytes("userId"));
            get.addColumn(getBytes("pl"), getBytes("parentId"));
            get.addColumn(getBytes("pl"), getBytes("content"));
            list.add(get);
        }
        return list;
    }

    /**
     * 封装配置的所有字段列族
     */
    private static Map<byte[], byte[]> packFamilyMap(Result result) {
        Map<byte[], byte[]> dataMap = null;
        dataMap = new LinkedHashMap<byte[], byte[]>();
        dataMap.putAll(result.getFamilyMap(getBytes("pl")));
        // dataMap.putAll(result.getFamilyMap(getBytes("timestamp")));  
        // dataMap.putAll(result.getFamilyMap(getBytes("value")));  
        return dataMap;
    }

    private static String toStr(byte[] bt) {
        return Bytes.toString(bt);
    }

    /**
     * 根据Key查询所有的值
     *
     * @param getList
     * @return
     * @throws IOException
     */
    public Result[] getList(List<Get> getList, String tableName) throws IOException {
        // 从表池中取出HBASE表对象  
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        return table.get(getList);
    }

    /* 转换byte数组 */
    public static byte[] getBytes(String str) {
        if (str == null)
            str = "";

        return Bytes.toBytes(str);
    }


    ////
    public static Map<String, Object> divicePage(String tableName, String startRow, String endRow, String lastRowKey, int num) throws IOException {

        Filter filter = new PageFilter(num);//每页展示条数
        Scan scan = new Scan();
        scan.setFilter(filter);
        byte[] postfix = Bytes.toBytes("postfix");
        if (lastRowKey != null) {
            // 注意这里添加了 POSTFIX 操作，不然死循环了
            //因为hbase的row是字典序列排列的，因此上一次的lastrow需要添加额外的0表示新的开始。另外startKey的那一行是包含在scan里面的
            byte[] start = Bytes.add(Bytes.toBytes(lastRowKey), postfix);
            scan.setStartRow(start);
        } else {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        //取不到 rowkey=endRowKey的行，开区间
        scan.setStopRow(Bytes.toBytes(endRow));

        ResultScanner rs = table.getScanner(scan);
        Result r = null;
        Map<String, Object> resultMap = new HashMap<>();

        List<Map<String, String>> recordList = new ArrayList<>();
        int index = 0;
        while ((r = rs.next()) != null) {
            lastRowKey = Bytes.toStringBinary(r.getRow());
            // System.out.println(Bytes.toString(lastRow));
            List<Cell> cells = r.listCells();
            Map<String, String> record = new TreeMap<>();
            for (int i = 0; i < cells.size(); i++) {
                String key = Bytes.toString(CellUtil.cloneQualifier(cells.get(i)));
                String value = Bytes.toString(CellUtil.cloneValue(cells.get(i)));
                record.put(key, value);
            }
            recordList.add(record);
            if (++index >= 10) break;
        }
        rs.close();
        resultMap.put("last_row", lastRowKey);
        resultMap.put("data", recordList);
        System.out.println(recordList.size());
        return resultMap;
    }

    @Test
    public void testt2() throws Exception {
        Map<String, Object> resultMap = divicePage("bigdata:comment", "00_10035_20180522092417682_null", "00_10393_20180522092417689_null", null, 10);
        System.out.println(resultMap);
    }
}