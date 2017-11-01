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
public class UserCount implements Analyzer {

    private static final Logger logger = Logger.getLogger(UserCount.class);

    private static final Gson gson = new GsonBuilder().create();
    private final Type mapType = new TypeToken<Map<String, Object>>() {

    }.getType();
    private static final String GET_USER_SQL = "SELECT userid FROM analytics_user_entry WHERE time = %d";
    private static final String GET_USER_COUNT_SQL = "SELECT count FROM analytics_unique_user_count WHERE time = %d";
    private static final String USER_INSERTION_SQL = "INSERT IGNORE INTO analytics_user_entry(time,userid) VALUES (?, ?)";
    private static final String UPDATE_USER_COUNT_SQL = "INSERT INTO analytics_unique_user_count(time,count)"
            + "SELECT time,count(*) AS count FROM analytics_user_entry GROUP BY time "
            + "ON DUPLICATE KEY UPDATE count = count+VALUES(count)";

    private static final String INSERT_USER_COUNT_SQL = "INSERT INTO analytics_unique_user_count (time, count) VALUES (?, ?)"
            + " ON DUPLICATE KEY UPDATE count = VALUES(count)";

    private static final String DELETE_USER_SQL = "DELETE FROM analytics_user_entry WHERE time >= ? and time < ?";
    private static final String DELETE_USER_COUNT_SQL = "DELETE FROM analytics_unique_user_count WHERE time >= ? and time < ?";

    private final Connection sqlConnection;
    private final Map<Long, Long> userCountMap = new HashMap<>();
    private final HashMap<Long, Set<Long>> userEntry = new HashMap<>();

    public UserCount(Connection sqlConnection) throws SQLException {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
        userEntry.clear();
        userCountMap.clear();

    }

    @Override
    public void processLog(List<LogBean> listLogBean) {
        long processingLongDay = 0L;
        Set<Long> processedUserIds = new HashSet<>();
        for (LogBean logBean : listLogBean) {
            long time = Long.parseLong(logBean.getTimestamp().substring(0, 8));
            if (processingLongDay != time) {
                processingLongDay = time;
                processedUserIds = getUserIds(processingLongDay);
                long userCount = getUserCount(processingLongDay);
                userCountMap.put(processingLongDay, userCount);

            }

            String method = logBean.getMethodName();
            String paramValue = logBean.getParams();

            Long userId = getUserId(paramValue, method);
            if (userId != null) {
                if (!processedUserIds.contains(userId)) {
                    buildUserEntry(time, userId);
                    processedUserIds.add(userId);

                    if (userCountMap.containsKey(time)) {
                        userCountMap.put(time, userCountMap.get(time) + 1L);
                    } else {
                        userCountMap.put(time, 1L);
                    }
                }

            }
        }
    }

    private void buildUserEntry(long time, long userid) {
        if (userEntry.containsKey(time)) {
            userEntry.get(time).add(userid);
        } else {
            Set<Long> list = new HashSet<>();
            list.add(userid);
            userEntry.put(time, list);
        }
    }

    @Override
    public void saveToDB() throws SQLException {
        insertUserEntry();
        updateUserCount();
    }

    @Override
    public void recalculate(long startTime, long endTime) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteFromDB(long startTime, long endTime) throws SQLException {
        try (PreparedStatement deleStmt = sqlConnection.prepareStatement(DELETE_USER_SQL)) {
            deleStmt.setLong(1, startTime);
            deleStmt.setLong(2, endTime);
            deleStmt.executeQuery();
            deleStmt.clearParameters();
        }

        try (PreparedStatement deleStmt = sqlConnection.prepareStatement(DELETE_USER_COUNT_SQL)) {
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

    private Set<Long> getUserIds(long time) {
        String sql = String.format(GET_USER_SQL, time);
        Set userIds = new HashSet<Long>();
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(sql)) {

            ResultSet rs = prepStmt.executeQuery();
            while (rs.next()) {
                long userid = rs.getLong("userid");
                userIds.add((userid));
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return userIds;
    }

    private long getUserCount(long time) {
        String sql = String.format(GET_USER_COUNT_SQL, time);
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(sql)) {
            ResultSet rs = prepStmt.executeQuery();
            if (rs.next()) {
                return rs.getLong("count");
            }
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return 0L;
    }

    private Long getUserId(String paramValue, String method) {
        String userIdKey = Constant.METHOD_USER_KEY.get(method);
        Long userId = null;
        try {
            if (userIdKey != null) {
                Map<String, Object> paramValueMap = gson.fromJson(paramValue, mapType);
                userId = gson.toJsonTree(paramValueMap.get(userIdKey)).getAsLong();
                return userId;
            }

        } catch (Exception ex) {
            logger.error("", ex);
        }

        return userId;
    }

    private void insertUserEntry() throws SQLException {

        int batchLimit = Tools.SQL_BATCH_LIMIT;
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(USER_INSERTION_SQL)) {
            for (Map.Entry<Long, Set<Long>> childEntry : userEntry.entrySet()) {
                for (long userid : childEntry.getValue()) {
                    prepStmt.setLong(1, childEntry.getKey());
                    prepStmt.setLong(2, userid);
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

    private void updateUserCount() throws SQLException {
        int batchLimit = Tools.SQL_BATCH_LIMIT;
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(INSERT_USER_COUNT_SQL)) {
            for (Map.Entry<Long, Long> entry : userCountMap.entrySet()) {
                prepStmt.setLong(1, entry.getKey());
                prepStmt.setLong(2, entry.getValue());
                prepStmt.addBatch();
                batchLimit -= 1;
                if (batchLimit <= 0) {
                    prepStmt.executeBatch();
                    prepStmt.clearBatch();
                    batchLimit = Tools.SQL_BATCH_LIMIT;
                }
            }
            prepStmt.executeBatch();
            prepStmt.clearBatch();
        }
    }

    private static class Constant {

        private static final Map<String, String> METHOD_USER_KEY = new HashMap<>();

        static {
            METHOD_USER_KEY.put("getMissedCallList", "calleeId");
            METHOD_USER_KEY.put("userOnlineStatus", "userId");
        }
    }
}
