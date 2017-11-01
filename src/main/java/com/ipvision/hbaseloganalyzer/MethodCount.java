/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer;

import com.google.common.annotations.VisibleForTesting;
import com.ipvision.analyzer.hbase.LogBean;
import com.ipvision.analyzer.utils.Tools;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author rakib
 */
public class MethodCount implements Analyzer {

    private final Connection sqlConnection;

    private final HashMap<String, HashMap<Long, Long>> countMap = new HashMap<>();
    private static final String METHOD_COUNT_SQL
            = "INSERT INTO analytics_method_count (method, time, count) VALUES (?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE count = count + VALUES (count)";
    private final String DELETE_METHOD_COUNT = "DELETE FROM analytics_method_count WHERE time >= ? and time < ?";

    public MethodCount(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

  

    @Override
    public void close() throws IOException {
        clear();
    }

    @Override
    public void clear() {
        this.countMap.clear();
    }

    @VisibleForTesting
    @Override
    public  void processLog(List<LogBean> listLogBean) {

        for (LogBean logBean : listLogBean) {
            if (logBean.getMethodName() != null) {
                Long time = Long.parseLong(logBean.getTimestamp().substring(0, 10));
                String method = logBean.getMethodName();

                HashMap<Long, Long> hm;
                if (countMap.containsKey(method)) {
                    hm = countMap.get(method);
                } else {
                    hm = new HashMap<>();
                    countMap.put(method, hm);
                }
                if (hm.containsKey(time)) {
                    hm.put(time, hm.get(time) + 1L);
                } else {
                    hm.put(time, 1L);
                }
            }
        }

    }

    @Override
    public void saveToDB() throws SQLException {
        int batchLimit = Tools.SQL_BATCH_LIMIT;
        try (PreparedStatement insertStmt = sqlConnection.prepareStatement(METHOD_COUNT_SQL)) {
            for (Map.Entry<String, HashMap<Long, Long>> parentEntry : countMap.entrySet()) {
                for (Map.Entry<Long, Long> childEntry : parentEntry.getValue().entrySet()) {
                    insertStmt.setString(1, parentEntry.getKey());
                    insertStmt.setLong(2, childEntry.getKey());
                    insertStmt.setLong(3, childEntry.getValue());
                    insertStmt.addBatch();
                    batchLimit -= 1;
                    if (batchLimit <= 0) {
                        insertStmt.executeBatch();
                        insertStmt.clearBatch();
                        batchLimit = Tools.SQL_BATCH_LIMIT;
                    }
                }

            }
            insertStmt.executeBatch();
            insertStmt.close();
        }

    }

    @Override
    public void recalculate(long startTime, long endTime) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteFromDB(long startTime, long endTime) throws SQLException {
        try (PreparedStatement deleStmt = sqlConnection.prepareStatement(DELETE_METHOD_COUNT)) {
            deleStmt.setLong(1, startTime);
            deleStmt.setLong(2, endTime);
            deleStmt.execute();
            deleStmt.clearParameters();
        }
    }

}
