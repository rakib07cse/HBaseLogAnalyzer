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
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

/**
 *
 * @author rakib
 */
public class OnlineUserStatus implements Analyzer {

    private static final Logger logger = Logger.getLogger(OnlineUserStatus.class);
    private final Map<Long, Map<String, Object>> userOnlineInfo = new HashMap<>();
    private static final Gson gson = new GsonBuilder().create();
    private final Type mapType = new TypeToken<Map<String, Object>>() {
    }.getType();
    private final Connection sqlConnection;
    private static final String USER_INSERTION_SQL = "INSERT IGNORE INTO analytics_user_online_status (time, userid, status) VALUES (?, ?, ?) ";
    private static final String DELETE_ONLINE_USER_STATUS_COUNT = "DELETE FROM analytics_user_online_status WHERE time >= ? and time < ?";
    private final String methodName = "userOnlineStatus";

    public OnlineUserStatus(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
        userOnlineInfo.clear();
    }

    @Override
    public void processLog(List<LogBean> listLogBean) {

        for (LogBean logBean : listLogBean) {
            if (logBean.getMethodName() != null) {
                if (logBean.getMethodName().equalsIgnoreCase(methodName)) {
                    long time = Long.parseLong(logBean.getTimestamp());
                    String paramValue = logBean.getParams();
                    if (!paramValue.equals("")) {
                        Map<String, Object> paramMap = stringToMap(paramValue);
                        userOnlineInfo.put(time, paramMap);
                    }
                }
            }
        }
    }

    private Map<String, Object> stringToMap(String source) {
        Map<String, Object> target = new HashMap<>();
        try {
            target = gson.fromJson(source, mapType);
        } catch (Exception ex) {
            logger.error("", ex);
        }
        return target;
    }

    @Override
    public void saveToDB() throws SQLException {
        int insertRow = 0;
        int batchLimit = Tools.SQL_BATCH_LIMIT;

        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(USER_INSERTION_SQL)) {
            for (Map.Entry<Long, Map<String, Object>> childEntry : userOnlineInfo.entrySet()) {
                Map<String, Object> param = childEntry.getValue();
                try {
                    prepStmt.setLong(1, childEntry.getKey());
                    prepStmt.setLong(2, gson.toJsonTree(param.get(Constant.USERID)).getAsLong());
                    prepStmt.setLong(3, gson.toJsonTree(param.get(Constant.STATUS)).getAsLong());
                } catch (Exception ex) {
                    logger.error("", ex);
                    continue;
                }
                prepStmt.addBatch();
                if (++insertRow % batchLimit == 0) {
                    prepStmt.executeBatch();
                    prepStmt.clearBatch();
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
        try (PreparedStatement deleStmt = sqlConnection.prepareStatement(DELETE_ONLINE_USER_STATUS_COUNT)) {
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

    private static class Constant {

        private static final String USERID = "userId";
        private static final String STATUS = "status";
    }
}
