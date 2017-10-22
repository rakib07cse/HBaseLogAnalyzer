/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer;

import com.ipvision.analyzer.hbase.LogBean;
import com.ipvision.analyzer.utils.Tools;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 *
 * @author rakib
 */
public class ActivityCount implements Analyzer {
    
    private final Map<String, Map<Long, Long>> countMap = new HashMap<>();
    private final Pattern pattern = Pattern.compile("^(\\d{17})\\s+INFO - R \\S+ (\\w+) .*$");
    private final String ACTIVITY_METHOD_SQL = "SELECT activity, method FROM analytics_activity_method_map";
    private final Map<String, Set<String>> methodActivityMap = new HashMap<>();
    private final Map<String, Set<String>> activityMethodMap = new HashMap<>();
    private final String ACTIVITY_COUNT_SQL = "INSERT INTO analytics_activity_count (activity, time, count) VALUES (?,?,?)"
            + " ON DUPLICATE KEY UPDATE count = count + VALUES(count)";
    private final String DELETE_ACTIVITY_COUNT = "DELETE FROM analytics_activity_count WHERE time >= ? and time < ? ";
    private final String INSERT_ACTIVITY_COUNT = "INSERT INTO analytics_activity_count (activity, time, count) "
            + "SELECT '%s', time, SUM(count) AS count FROM analytics_method_count "
            + "WHERE method IN %s AND time >= ? AND time < ? GROUP BY time";
    private final Connection sqlConnection;
    
    public ActivityCount(Connection sqlConnection) throws SQLException {
        this.sqlConnection = sqlConnection;
        initActivity();
    }
    
    private void initActivity() throws SQLException {
        
        try (PreparedStatement statement = sqlConnection.prepareStatement(ACTIVITY_METHOD_SQL)) {
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                String activity = rs.getString("activity");
                String method = rs.getString("method");
                buildActivity(activity, method);
                
            }
        }
    }
    
    private void buildActivity(String activity, String method) {
        if (methodActivityMap.containsKey(method)) {
            methodActivityMap.get(method).add(activity);
        } else {
            Set<String> list = new HashSet<String>();
            list.add(activity);
            methodActivityMap.put(method, list);
        }
    }
    
    @Override
    public void clear() {
        countMap.clear();
    }
    
    @Override
    public void processLog(List<LogBean> listLogBean) {
        for (LogBean logBean : listLogBean) {
            Long time = Long.parseLong(logBean.getTimestamp().substring(0, 10));
            String method = logBean.getMethodName();
            if (methodActivityMap.containsKey(method)) {
                Collection<String> activities = methodActivityMap.get(method);
                updateCounter(activities, time);
            }
        }
    }
    
    private void updateCounter(Collection<String> activities, Long time) {
        Map<Long, Long> childMap;
        for (String activity : activities) {
            if (countMap.containsKey(activity)) {
                childMap = countMap.get(activity);
            } else {
                childMap = new HashMap<Long, Long>();
                countMap.put(activity, childMap);
            }
            
            if (childMap.containsKey(time)) {
                childMap.put(time, childMap.get(time) + 1L);
            } else {
                childMap.put(time, 1L);
            }
        }
    }
    
    @Override
    public void saveToDB() throws SQLException {
        int insertRow = 0;
        int batchLimit = Tools.SQL_BATCH_LIMIT;
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(ACTIVITY_COUNT_SQL)) {
            for (Map.Entry<String, Map<Long, Long>> parentEntry : countMap.entrySet()) {
                Map<Long, Long> childEntry = parentEntry.getValue();
                for (Map.Entry<Long, Long> entry : childEntry.entrySet()) {
                    prepStmt.setString(1, parentEntry.getKey());
                    prepStmt.setLong(2, entry.getKey());
                    prepStmt.setLong(3, entry.getValue());
                    prepStmt.addBatch();
                    
                    if (++insertRow % batchLimit == 0) {
                        prepStmt.executeBatch();
                        prepStmt.clearBatch();
                    }
                }
                
            }
            prepStmt.executeBatch();
            prepStmt.clearBatch();
        }
    }
    
    @Override
    public void recalculate(long startTime, long endTime) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void deleteFromDB(long startTime, long endTime) throws SQLException {
        try (PreparedStatement delStmt = sqlConnection.prepareStatement(DELETE_ACTIVITY_COUNT)) {
            delStmt.setLong(1, startTime);
            delStmt.setLong(2, endTime);
            delStmt.execute();
            delStmt.clearParameters();
        }
    }
    
    @Override
    public void close() throws IOException {
        clear();
    }
    
}
