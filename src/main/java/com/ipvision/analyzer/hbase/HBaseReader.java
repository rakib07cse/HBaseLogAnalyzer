/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.analyzer.hbase;

import com.ipvision.analyzer.utils.Tools;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author rakib
 */
public class HBaseReader {

    public static List<LogBean> processHBaseTable(HTableDescriptor tmpTablenName) throws IOException {

//        HTableDescriptor[] tmpTablenNames = HBaseManager.getHBaseManager().getAdmin().listTables(Tools.HBASE_TMP_TABLE_PATTERN);
        List<LogBean> listLogBean = new ArrayList<>();
        if (HBaseManager.getHBaseManager().getAdmin().isTableEnabled(tmpTablenName.getNameAsString())) {
            listLogBean = readHBaseTable(tmpTablenName.getNameAsString());
            if (!listLogBean.isEmpty()) {
                HBaseManager.getHBaseManager().getAdmin().disableTable(tmpTablenName.getNameAsString());
            }

            //processLogBean(allAnalyzers, listLogBean);
        }

        return listLogBean;
    }

    private static List<LogBean> readHBaseTable(String tableName) throws IOException {

        HTable table = HBaseManager.getHBaseManager().createHTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan());

        List<LogBean> listLogBean = new ArrayList<>();

        for (Result row : scanner) {
            LogBean logBean = new LogBean();
            String rowKey = Bytes.toString(row.getRow());
            logBean.setTimestamp(rowKey);
            for (byte[] columeFamaily : row.getMap().keySet()) {
                for (byte[] cols : row.getMap().get(columeFamaily).keySet()) {
                    String colName = Bytes.toString(cols);
                    for (long colValue : row.getMap().get(columeFamaily).get(cols).keySet()) {
                        String value = new String(row.getMap().get(columeFamaily).get(cols).get(colValue));
                        if(colName.equalsIgnoreCase(Tools.HBASE_TABLE_LIVESTREAMHISTORY_SECOND_COLUME_NAME)){
                            logBean.setLiveStreamParams(value);
                        }
                        if(colName.equalsIgnoreCase(Tools.HBASE_TABLE_LIVESTREAMHISTORY_FIRST_COLUME_NAME)){
                            logBean.setLiveStreamHistory(value);
                        }
                        if (colName.equalsIgnoreCase(Tools.HBASE_TABLE_METHOD_FIRST_COLUME_NAME)) {
                            logBean.setMethodName(value);
                        }
                        if (colName.equalsIgnoreCase(Tools.HBASE_TABLE_METHOD_SECOND_COLUME_NAME)) {
                            logBean.setParams(value);
                        }
                        if(colName.equalsIgnoreCase(Tools.HBASE_TABLE_NOTINOF_FIRST_COLUME_NAME)){
                            logBean.setLogLevel(value);
                        }
                        if(colName.equalsIgnoreCase(Tools.HBASE_TABLE_NOTINFO_SECOND_COLUME_NAME)){
                            logBean.setEventType(value);
                        }
                        
                    }

                }
            }
            listLogBean.add(logBean);
        }

        return listLogBean;

    }

}
