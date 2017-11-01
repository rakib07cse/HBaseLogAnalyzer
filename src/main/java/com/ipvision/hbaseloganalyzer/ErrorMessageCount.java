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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 *
 * @author rakib
 */
public class ErrorMessageCount implements Analyzer {

    private final Pattern errorPattern = Pattern.compile("^(\\d{17})\\s+(FATAL|ERROR|WARN)\\s+(.*)$");
    private static final String requestIdPattern = " ?" + Tools.UUID_PATTERN;

    private final HashMap<MessageWithType, HashMap<Long, Long>> countMap
            = new HashMap<>();

    private static final String ERR_MSG_COUNT_SQL
            = "INSERT INTO analytics_error_message_count (type, hashcode, message, time, count) "
            + "VALUES (?, ?, ?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE count = count + VALUES (count)";

    private static final String DELETE_ERROR_MESSAGE_COUNT = "DELETE FROM analytics_error_message_count WHERE time >= ? and time < ?";

    private final Connection sqlConnection;

    public ErrorMessageCount(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
       countMap.clear();
    }

    @Override
    public void processLog(List<LogBean> listLogBean) {

        for (LogBean logBean : listLogBean) {
            if (logBean.getLogLevel()!= null &&(logBean.getLogLevel().equalsIgnoreCase("ERROR") || logBean.getLogLevel().equalsIgnoreCase("WARN") || logBean.getLogLevel().equalsIgnoreCase("FATAL"))) {
                Long time = Long.parseLong(logBean.getTimestamp().substring(0, 10));
                String type = logBean.getLogLevel();
                String text = logBean.getEventType();
                text = text.replaceAll(requestIdPattern, "");
                MessageWithType message = new MessageWithType(type, text);

                HashMap<Long, Long> hm;
                if (countMap.containsKey(message)) {
                    hm = countMap.get(message);
                } else {
                    hm = new HashMap<>();
                    countMap.put(message, hm);
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
        try (PreparedStatement prepStmt = sqlConnection.prepareStatement(ERR_MSG_COUNT_SQL)) {
            int batchLimit = Tools.SQL_BATCH_LIMIT;
            for (Map.Entry<MessageWithType, HashMap<Long, Long>> parentEntry : countMap.entrySet()) {
                MessageWithType message = parentEntry.getKey();
                for (Map.Entry<Long, Long> childEntry : parentEntry.getValue().entrySet()) {
                    Long time = childEntry.getKey();
                    Long count = childEntry.getValue();

                    String type = message.getType();
                    prepStmt.setString(1, type);
                    String text = message.getMessage();
                    int hashcode = text.hashCode();
                    prepStmt.setInt(2, hashcode);
                    prepStmt.setString(3, text);
                    prepStmt.setLong(4, time);
                    prepStmt.setLong(5, count);

                    prepStmt.addBatch();
                    prepStmt.clearParameters();

                    batchLimit -= 1;
                    
                    if(batchLimit <=0){
                        prepStmt.executeBatch();
                        prepStmt.clearBatch();
                        batchLimit = Tools.SQL_BATCH_LIMIT;
                    }

                }
                prepStmt.executeBatch();
                prepStmt.clearBatch();
            }
        }
    }

    @Override
    public void recalculate(long startTime, long endTime) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void deleteFromDB(long startTime, long endTime) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private class MessageWithType extends Object {

        private final String type;
        private final String message;

        public MessageWithType(String type, String message) {
            this.type = type;
            this.message = message;
        }

        public String getType() {
            return type;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public int hashCode() {
            return this.message.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (null == obj) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            final MessageWithType other = (MessageWithType) obj;
            if (!Objects.equals(this.type, other.type)) {
                return false;
            }
            return Objects.equals(this.message, other.message);
        }
    }
}
