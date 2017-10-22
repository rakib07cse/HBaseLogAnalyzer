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
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


/**
 *
 * @author rakib
 */
public class LiveStreamHistory implements Analyzer {

    private static final Logger logger = Logger.getLogger(LiveStreamHistory.class);
    private final Type mapType = new TypeToken<Map<String, Object>>() {
    }.getType();

    private static final Gson gson = new GsonBuilder().create();
    private final Connection sqlConnection;
    private static final String ZERO_TIMESTAMP = "00000000000000000";
    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    private final List<Map<String, Object>> dtoValueStore = new ArrayList<>();
    private static final String QUERY_TEMPLATE = "INSERT INTO analytices_live_stream (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";

    public LiveStreamHistory(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void processLog(List<LogBean> listLogBean) {
        for (LogBean logBean : listLogBean) {
            if (logBean.getLiveStreamHistory().equalsIgnoreCase("LiveStreamHistory")) {
                String timeStamp = logBean.getTimestamp();
                String liveStreamParams = logBean.getLiveStreamParams();
                Map<String, Object> dtoValue = gson.fromJson(liveStreamParams, mapType);
                dtoValue.put("logtime", toTimeStamp(timeStamp));
                dtoValueStore.add(dtoValue);
            }
        }
    }

    @Override
    public void saveToDB() throws SQLException {

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

    private Object toTimeStamp(String timeStamp) {
        try {
            Date date = sdf.parse(timeStamp.concat(ZERO_TIMESTAMP).substring(0, 17));
            return date.getTime();
        } catch (ParseException ex) {
            logger.error("", ex);
            return 0L;
        }
    }

}
