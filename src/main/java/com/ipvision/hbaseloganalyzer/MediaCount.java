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
import org.ringid.newsfeeds.CassAlbumDTO;
import org.ringid.newsfeeds.FeedDTO;
import org.ringid.utilities.AppConstants;

/**
 *
 * @author rakib
 */
public class MediaCount implements Analyzer {

    private static final Logger logger = Logger.getLogger(MediaCount.class);
    private static final String IMAGE = "IMAGE";
    private static final String AUDIO = "AUDIO";
    private static final String VIDEO = "VIDEO";

    private static final Gson gson = new GsonBuilder().serializeNulls().create();

    private final Pattern pattern = Pattern.compile("^(\\d{17})\\s+INFO - R \\S+ (\\w+) - (.*)$");

    private final Type mapType = new TypeToken<Map<String, Object>>() {
    }.getType();

    private final Map<String, Map<Long, Long>> countMap = new HashMap<>();

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
            int count = 0;
            if ((logBean.getMethodName() != null) && (logBean.getMethodName().equalsIgnoreCase("addStatus") || logBean.getMethodName().equalsIgnoreCase("addProfileOrCoverImage"))) {
                Long time = Long.parseLong(logBean.getTimestamp().substring(0, 10));

                if (logBean.getMethodName().equalsIgnoreCase("addStatus")) {
                    try {
                        Map<String, Object> map = gson.fromJson(logBean.getParams(), mapType);
                        Object feedDtoValue = map.get("feedDTO");

                        FeedDTO dto = gson.fromJson(gson.toJsonTree(feedDtoValue), FeedDTO.class);
                        CassAlbumDTO albumDTO = dto.getAlbumDTO();
                        if (null != albumDTO) {
                            switch (dto.getContentType()) {
                                case AppConstants.SINGLE_IMAGE:
                                case AppConstants.SINGLE_IMAGE_WITH_ALBUM:
                                case AppConstants.MULTIPLE_IMAGE_WITH_ALBUM:
                                    if (null != albumDTO.getImgDTOs()) {
                                        type = IMAGE;
                                        count = albumDTO.getImgDTOs().size();
                                    }
                                    break;
                                case AppConstants.SINGLE_AUDIO:
                                case AppConstants.SINGLE_AUDIO_WITH_ALBUM:
                                case AppConstants.MULTIPLE_AUDIO_WITH_ALBUM:
                                    if (null != albumDTO.getMultiMediaDTOs()) {
                                        type = AUDIO;
                                        count = albumDTO.getMultiMediaDTOs().size();
                                    }
                                    break;
                                case AppConstants.SINGLE_VIDEO:
                                case AppConstants.SINGLE_VIDEO_WITH_ALBUM:
                                case AppConstants.MULTIPLE_VIDEO_WITH_ALBUM:
                                    if (null != albumDTO.getMultiMediaDTOs()) {
                                        type = VIDEO;
                                        count = albumDTO.getMultiMediaDTOs().size();
                                    }
                            }
                        }
                    } catch (Exception ex) {
                        logger.error("", ex);
                    }
                } else if (logBean.getMethodName().equalsIgnoreCase("addProfileOrCoverImage")) {
                    type = IMAGE;
                    count = 1;
                }
                if (null != type && 0 < count) {
                    Map<Long, Long> hm;
                    if (countMap.containsKey(type)) {
                        hm = countMap.get(type);
                    } else {
                        hm = new HashMap<>();
                        countMap.put(type, hm);
                    }
                    if (hm.containsKey(time)) {
                        hm.put(time, hm.get(time) + count);
                    } else {
                        hm.put(time, (long) count);
                    }

                }

            }
        }

    }

    @Override
    public void saveToDB() throws SQLException {
        try (PreparedStatement prepStmt = sqlConnection.prepareCall(MEDIA_COUNT_SQL)) {
            int batchLimit = Tools.SQL_BATCH_LIMIT;
            for (Map.Entry<String, Map<Long, Long>> parentEntry : countMap.entrySet()) {
                String type = parentEntry.getKey();
                for (Map.Entry<Long, Long> childEntry : parentEntry.getValue().entrySet()) {
                    Long time = childEntry.getKey();
                    Long count = childEntry.getValue();

                    prepStmt.setString(1, type);
                    prepStmt.setLong(2, time);
                    prepStmt.setLong(3, count);
                    prepStmt.addBatch();
                    prepStmt.clearParameters();
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
}
