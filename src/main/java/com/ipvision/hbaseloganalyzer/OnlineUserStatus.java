/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer;

import com.ipvision.analyzer.hbase.LogBean;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
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
    private final Pattern pattern = Pattern.compile("^(\\d{17})\\s+INFO - R \\S+ userOnlineStatus - (.*)$");
    private final Map<Long, Map<String, Object>> userOnlineInfo = new HashMap<>();
    private Connection sqlConnection;
    private static final String USER_INSERTION_SQL = "INSERT IGNORE INTO analytics_user_online_status (time, userid, status) VALUES (?, ?, ?) ";
    private static final String DELETE_ONLINE_USER_STATUS_COUNT = "DELETE FROM analytics_user_online_status WHERE time >= ? and time < ?";

    public void OnlineUserStatus(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void processLog(List<LogBean> listLogBean) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void saveToDB() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

}
