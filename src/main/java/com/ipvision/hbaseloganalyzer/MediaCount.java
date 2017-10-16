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
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 * @author rakib
 */
public class MediaCount implements Analyzer {

    private static final String IMAGE = "IMAGE";
    private static final String AUDIO = "AUDIO";
    private static final String VIDEO = "VIDEO";

    private static final Gson gson = new GsonBuilder().serializeNulls().create();

    private final Pattern pattern = Pattern.compile("^(\\d{17})\\s+INFO - R \\S+ (\\w+) - (.*)$");

    private final Type mapType = new TypeToken<Map<String, Object>>() {
    }.getType();

    private final Map<String, Map<Long, Long>> countMap = new HashMap<String, Map<Long, Long>>();

    private static final String MEDIA_COUNT_SQL
            = "INSERT INTO analytics_media_count (type, time, count) VALUES (?, ?, ?) "
            + "ON DUPLICATE KEY UPDATE count = count + VALUES (count)";

    private final String DELETE_MEDIA_COUNT = "DELETE FROM analytics_media_count WHERE time >= ? and time < ?";

    private final Connection sqlConnection;

    public MediaCount(Connection sqlConnection) throws SQLException {
        this.sqlConnection = sqlConnection;
    }

    @Override
    public void clear() {
        this.countMap.clear();
    }

    @Override
    public void processLog(List<LogBean> listLogBean) {
        for (LogBean logBean : listLogBean) {
            String type = null;
            if (logBean.getMethod().equalsIgnoreCase("addStatus") || logBean.getMethod().equalsIgnoreCase("addProfileOrCoverImage")) {
                Long time = Long.parseLong(logBean.getTimestamp().substring(0, 10));
                if (logBean.getMethod().equalsIgnoreCase("addStatus")) {
                    Map<String, Object> map = gson.fromJson(logBean.getData(), mapType);
                    Object feedDtoValue = map.get("feedDTO");

               //     FeedDTO dto = gson.fromJson(gson.toJsonTree(feedDtoValue), FeedDTO.class);

                }

            }
        }

     

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
