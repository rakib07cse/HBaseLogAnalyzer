/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.ipvision.analyzer.hbase.LogBean;
import com.ipvision.analyzer.utils.Tools;
import com.ipvision.hbaselog.HBaseAnalyzerManager;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 *
 * @author rakib
 */
public class LiveViewerCount implements Analyzer {

    private static final Logger logger = Logger.getLogger(LiveViewerCount.class);
    private final Connection sqlConnection;
    private final Type mapType = new TypeToken<Map<String, Object>>() {
    }.getType();

    private static final Gson gson = new GsonBuilder().create();
    private final HashMap<Long, Set<Long>> viewerEntry = new HashMap<>();
    private final Map<Long, Long> viewerCountMap = new HashMap<>();
    private final String GET_VIEWER_SQL = "SELECT viewerid FROM analytics_live_viewer_entry WHERE time= %d";
    private final String GET_VIEWER_COUNT_SQL = "SELECT count FROM analytics_unique_live_viewr_count WHERE time=%d";

    private final String VIEWER_INSERTION_SQL = "INSERT IGNORE INTO analytics_live_viewer_entry(time,viewerid) VALUES(?,?)";
    private final String INSERT_VIEWER_COUNT_SQL = "INSERT INTO analytics_unique_live_viewer_count(time,count) VALUES(?,?)"
            + "ON DUPLICATE KEY UPDATE count = VALUES(count)";

    private static final String DELETE_VIEWER_SQL = "DELETE FROM analytics_live_viewer_entry WHERE time>=? and time <?";
    private static final String DELETE_VIEWER_COUNT_SQL = "DELETE FROM analytics_unique_live_viewer_count WHERE time>=? and time<?";

    public LiveViewerCount(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
        viewerCountMap.clear();
        viewerEntry.clear();
    }

    @Override
    public void processLog(List<LogBean> listLogBean) {
        long latestLogTime = 0L;
        Set<Long> viewerIds = new HashSet<>();
        for (LogBean logBean : listLogBean) {
            if (logBean.getMethodName() != null) {
                if (logBean.getMethodName().equalsIgnoreCase("updateStreamViewCount")) {
                    long time = Long.parseLong(logBean.getTimestamp().substring(0, 8));
                    String paramValue = logBean.getParams();
                    Long viewerId = getViewerId(paramValue);
                    if (viewerId != null) {
                        if (latestLogTime != time) {
                            latestLogTime = time;
                            viewerIds = getViewerIds(latestLogTime);
                            long viewerCount = getViewerCount(latestLogTime);
                            viewerCountMap.put(latestLogTime, viewerCount);
                        }
                    }
                    if (!viewerIds.contains(viewerId)) {
                        buildViewerEntry(time, viewerId);
                        viewerIds.add(viewerId);
                        updateCount(time, 1);
                    }
                }
            }
        }
    }

    @Override
    public void saveToDB() throws SQLException {
        insertViewerEntry();
        updateViewerCount();

    }

    @Override
    public void recalculate(long startTime, long endTime) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteFromDB(long startTime, long endTime) throws SQLException {
        try (PreparedStatement deleStmt = sqlConnection.prepareStatement(DELETE_VIEWER_SQL)) {
            deleStmt.setLong(1, startTime);
            deleStmt.setLong(2, endTime);
            deleStmt.execute();
            deleStmt.clearParameters();
        }
        try (PreparedStatement deleStmt = sqlConnection.prepareStatement(DELETE_VIEWER_COUNT_SQL)) {
            deleStmt.setLong(1, startTime);
            deleStmt.setLong(2, endTime);
            deleStmt.execute();
            deleStmt.clearParameters();
        }
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private Long getViewerId(String paramValue) {
        Long viewerId = null;
        try {
            Map<String, Object> paramValueMap = gson.fromJson(paramValue, mapType);
            if (paramValueMap.containsKey("ssnUserId")) {
                viewerId = gson.toJsonTree(paramValueMap.get("ssnUserId")).getAsLong();
            } else if (paramValueMap.containsKey("sessionUserId")) {
                viewerId = gson.toJsonTree(paramValueMap.get("sessionUserId")).getAsLong();
            }
            return viewerId;
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return viewerId;
    }

    private Set<Long> getViewerIds(long time) {
        String sql = String.format(GET_VIEWER_SQL, time);
        Set<Long> viewerIds = new HashSet<>();
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(sql)) {
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                long viewerid = rs.getLong("viewerid");
                viewerIds.add(viewerid);
            }
        } catch (SQLException ex) {
            logger.error("", ex);
        }
        return viewerIds;
    }

    private long getViewerCount(long latestLogTime) {
        String sql = String.format(GET_VIEWER_COUNT_SQL, latestLogTime);
        long viewerCount = 0;
        try (PreparedStatement prepStmt = sqlConnection.prepareCall(sql)) {
            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                viewerCount = rs.getLong("count");
            }
        } catch (SQLException ex) {
            logger.error("", ex);
        }
        return viewerCount;
    }

    private void buildViewerEntry(long time, Long viewerId) {
        if (viewerEntry.containsKey(time)) {
            viewerEntry.get(time).add(viewerId);
        } else {
            Set<Long> list = new HashSet();
            list.add(viewerId);
            viewerEntry.put(time, list);
        }
    }

    private void updateCount(long time, long value) {
        if (viewerCountMap.containsKey(time)) {
            viewerCountMap.put(time, viewerCountMap.get(time) + value);
        } else {
            viewerCountMap.put(time, value);
        }
    }

    private void insertViewerEntry() throws SQLException {

        int batchLimit = Tools.SQL_BATCH_LIMIT;

        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(VIEWER_INSERTION_SQL)) {
            for (Map.Entry<Long, Set<Long>> childEntry : viewerEntry.entrySet()) {
                for (long viewerId : childEntry.getValue()) {
                    prepStmt.setLong(1, childEntry.getKey());
                    prepStmt.setLong(2, viewerId);
                    prepStmt.addBatch();
                    batchLimit -= 1;
                    if (batchLimit <= 0) {
                        prepStmt.executeBatch();
                        prepStmt.clearBatch();
                        batchLimit = Tools.SQL_BATCH_LIMIT;
                    }
                }
            }
            prepStmt.executeBatch();
            prepStmt.clearBatch();
        }
    }

    private void updateViewerCount() throws SQLException {
        int insertRow = 0;
        int batchLimit = Tools.SQL_BATCH_LIMIT;
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(INSERT_VIEWER_COUNT_SQL)) {
            for (Map.Entry<Long, Long> entry : viewerCountMap.entrySet()) {
                prepStmt.setLong(1, entry.getKey());
                prepStmt.setLong(2, entry.getValue());

                prepStmt.addBatch();
                if (++insertRow % batchLimit == 0) {
                    prepStmt.addBatch();
                    prepStmt.executeBatch();
                }
            }
            prepStmt.executeBatch();
            prepStmt.clearBatch();

        }
    }

}
