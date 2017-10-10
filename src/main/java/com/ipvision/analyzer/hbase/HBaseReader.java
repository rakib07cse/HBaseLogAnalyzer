/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.analyzer.hbase;

import com.ipvision.analyzer.utils.Tools;
import com.ipvision.hbaseloganalyzer.Analyzer;
import com.ipvision.hbaseloganalyzer.MethodCount;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
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

    public void processHBaseTable(Connection sqlConnection) throws IOException, SQLException {

        List<Analyzer> allAnalyzers = new ArrayList<Analyzer>() {
            {
                add(new MethodCount(sqlConnection));
            }
        };

        HTableDescriptor[] tmpTablenNames = HBaseManager.getHBaseManager().getAdmin().listTables(Tools.HBASE_TMP_TABLE_PATTERN);
        List<LogBean> listLogBean = null;
        for (HTableDescriptor tmpTablenName : tmpTablenNames) {
            listLogBean = new ArrayList<>();
            if (HBaseManager.getHBaseManager().getAdmin().isTableEnabled(tmpTablenName.getNameAsString())) {
                listLogBean = readHBaseTable(tmpTablenName.getNameAsString());
                if (!listLogBean.isEmpty()) {
                    HBaseManager.getHBaseManager().getAdmin().disableTable(tmpTablenName.getNameAsString());
                }

            }

            processLogBean(allAnalyzers, listLogBean);

        }

    }

    private List<LogBean> readHBaseTable(String tableName) throws IOException {

        HTable table = HBaseManager.getHBaseManager().createHTable(tableName);
        ResultScanner scanner = table.getScanner(new Scan());

        List<LogBean> listLogBean = new ArrayList<>();

        for (Result row : scanner) {
            LogBean logBean = new LogBean();
            String rowKey = Bytes.toString(row.getRow());
            logBean.setTimestmap(rowKey);
            for (byte[] columeFamaily : row.getMap().keySet()) {
                for (byte[] cols : row.getMap().get(columeFamaily).keySet()) {
                    String colName = Bytes.toString(cols);
                    for (long colValue : row.getMap().get(columeFamaily).get(cols).keySet()) {
                        String value = new String(row.getMap().get(columeFamaily).get(cols).get(colValue));
                        if (colName.equalsIgnoreCase(Tools.HBASE_TABLE_FIRST_COLUME_NAME)) {
                            logBean.setMethod(value);
                        }
                        if (colName.equalsIgnoreCase(Tools.HBASE_TABLE_SECOND_COLUME_NAME)) {
                            logBean.setData(value);
                        }
                    }

                }
            }
            listLogBean.add(logBean);
        }

        return listLogBean;

    }

    private void processLogBean(List<Analyzer> allAnalyzers, List<LogBean> listLogBean) throws SQLException {
        
        for (Analyzer analyzer : allAnalyzers) {
            analyzer.processLog(listLogBean);
            analyzer.saveToDB();
            analyzer.clear();
            
        }

    }
}
