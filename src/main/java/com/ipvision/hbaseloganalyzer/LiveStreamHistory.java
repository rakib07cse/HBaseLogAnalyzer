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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import java.lang.reflect.Type;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

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
    private static final String QUERY_TEMPLATE = "INSERT INTO analytics_live_stream (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s";
    
    public LiveStreamHistory(Connection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }
    
    @Override
    public void clear() {
        dtoValueStore.clear();
    }
    
    @Override
    public void processLog(List<LogBean> listLogBean) {
        for (LogBean logBean : listLogBean) {
          //  System.out.println("History: "+logBean.getLiveStreamHistory());
            if (logBean.getLiveStreamHistory() != null && logBean.getLiveStreamHistory().equalsIgnoreCase("LiveStreamHistory")) {
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
        try (Statement stmt = sqlConnection.createStatement()) {
            int batchLimit = Tools.SQL_BATCH_LIMIT;
            for (Map<String, Object> dtoValue : dtoValueStore) {
                String query = buildQuery(dtoValue);
                stmt.addBatch(query);
                batchLimit -= 1;
                if (batchLimit <= 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    batchLimit = Tools.SQL_BATCH_LIMIT;
                }
            }
            stmt.executeBatch();
            stmt.clearBatch();
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
        clear();
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
    
    private String buildQuery(Map<String, Object> dtoValue) {
        StringBuilder aggColumn = new StringBuilder();
        StringBuilder aggValue = new StringBuilder();
        StringBuilder aggUpdate = new StringBuilder();
        
        boolean first = true;
        String updateFormat = "%s=VALUES(%s)";
        
        for (Map.Entry<String, String> entry : Constant.KEY_COL_NAME.entrySet()) {
            String logKey = entry.getKey();
            String column = entry.getValue();
            Object logValue = dtoValue.get(logKey);
            
            if (logValue != null) {
                if (first) {
                    first = false;
                } else {
                    aggColumn.append(",");
                    aggValue.append(",");
                    aggUpdate.append(",");
                }
                aggColumn.append(column);
                aggUpdate.append(String.format(updateFormat, column, column));
                if (Constant.STR_TYPE_COL.contains(column)) {
                    String value = logValue.toString().replace("\"", "\\\"");
                    aggValue.append("\"");
                    aggValue.append(value);
                    aggValue.append("\"");
                } else {
                    aggValue.append(logValue);
                }
                
            }
        }
        
        if (aggValue.length() <= 0 || aggColumn.length() <= 0) {
            return null;
        }
        
        String query = String.format(QUERY_TEMPLATE, aggColumn, aggValue, aggUpdate);
        return query;
    }
    
    private static class Constant {
        
        private static final List<String> STR_TYPE_COL = Arrays.asList("country", "chatserverip",
                "streamserverip", "tags", "viewerserverip", "profileimage", "title", "streamid", "name");
        private static final Map<String, String> KEY_COL_NAME = new HashMap<>();
        
        static {
            KEY_COL_NAME.put("cnty", "country");
            KEY_COL_NAME.put("chatport", "chatport");
            KEY_COL_NAME.put("chatserverIp", "chatserverip");
            KEY_COL_NAME.put("streamIp", "streamserverip");
            KEY_COL_NAME.put("streamPort", "streamport");
            KEY_COL_NAME.put("catList", "tags");
            KEY_COL_NAME.put("isVrfid", "userstatus");
            KEY_COL_NAME.put("vwrIp", "viewerserverip");
            KEY_COL_NAME.put("vwrPort", "viewerserverport");
            KEY_COL_NAME.put("dvcc", "devicecategory");
            KEY_COL_NAME.put("endTm", "endTime");
            KEY_COL_NAME.put("giftOn", "gifton");
            KEY_COL_NAME.put("type", "isfeatured");
            KEY_COL_NAME.put("lat", "latitude");
            KEY_COL_NAME.put("lc", "likecount");
            KEY_COL_NAME.put("lon", "longitude");
            KEY_COL_NAME.put("fn", "name");
            KEY_COL_NAME.put("prIm", "profileimage");
            KEY_COL_NAME.put("uId", "ringid");
            KEY_COL_NAME.put("stTm", "starttime");
            KEY_COL_NAME.put("ttl", "title");
            KEY_COL_NAME.put("utId", "userid");
            KEY_COL_NAME.put("streamId", "streamid");
            KEY_COL_NAME.put("vwc", "viewcount");
            KEY_COL_NAME.put("coin", "startcoin");
            KEY_COL_NAME.put("endCoin", "endcoin");
            KEY_COL_NAME.put("utTyp", "userType");
            KEY_COL_NAME.put("rmid", "roomid");
            KEY_COL_NAME.put("device", "device");
            KEY_COL_NAME.put("tariff", "tariff");
            KEY_COL_NAME.put("ftrdScr", "featuredScore");
            KEY_COL_NAME.put("mType", "streamMediaType");
            KEY_COL_NAME.put("logtime", "logtime");
        }
    }
    
}
