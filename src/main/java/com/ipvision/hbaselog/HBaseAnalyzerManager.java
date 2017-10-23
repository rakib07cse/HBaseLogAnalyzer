/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaselog;

import com.ipvision.analyzer.hbase.HBaseLogBeanProcessor;
import com.ipvision.analyzer.hbase.HBaseManager;
import com.ipvision.analyzer.hbase.HBaseReader;
import com.ipvision.analyzer.hbase.HBaseWriter;
import com.ipvision.analyzer.hbase.LogBean;
import com.ipvision.analyzer.utils.Settings;
import com.ipvision.analyzer.utils.Tools;
import com.ipvision.hbaseloganalyzer.ActivityCount;
import com.ipvision.hbaseloganalyzer.Analyzer;
import com.ipvision.hbaseloganalyzer.ErrorMessageCount;
import com.ipvision.hbaseloganalyzer.LiveStreamHistory;
import com.ipvision.hbaseloganalyzer.LiveViewerCount;
import com.ipvision.hbaseloganalyzer.MethodCount;
import com.ipvision.hbaseloganalyzer.OnlineUserStatus;
import com.ipvision.hbaseloganalyzer.UserCount;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.log4j.Logger;

/**
 *
 * @author rakib
 */
public class HBaseAnalyzerManager {

    private static  Logger logger = Logger.getLogger(HBaseAnalyzerManager.class);
    private final Connection sqlConnection;
    private List<Analyzer> allAnalyzers = new ArrayList<>();
    private final String DATE_FORMAT = "yyyyMMddHH";

    public HBaseAnalyzerManager(String configFilepath) throws Exception {
        Properties properties = loadProperties(configFilepath);

        sqlConnection = createSqlConnection(properties);
        allAnalyzers = createAnalyzer();
    }

    private Properties loadProperties(String filepath) throws Exception {
        InputStream input = null;
        try {
            File file = new File(filepath);
            if (file.exists()) {
                input = new FileInputStream(file);
            } else {
                input = Thread.currentThread().getContextClassLoader().getResourceAsStream(filepath);
            }

            Properties properties = new Properties();
            properties.load(input);
            return properties;
        } catch (Exception ex) {
            String msg = String.format("Exception occured while loading configuration from file \"%s\"", filepath);
            throw new Exception(msg, ex);
        } finally {
            if (null != input) {
                input.close();
            }
        }
    }

    private Connection createSqlConnection(Properties properties) throws Exception {
        try {
            String mysqlHost = properties.getProperty(Tools.MYSQL_HOST_KEY);
            int port = Integer.parseInt(properties.getProperty(Tools.MYSQL_PORT_KEY));
            String db = properties.getProperty(Tools.MYSQL_DB_KEY);
            String user = properties.getProperty(Tools.MYSQL_USER_KEY);
            String passwd = properties.getProperty(Tools.MYSQL_PASSWD_KEY);

            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(
                    "jdbc:mysql://" + mysqlHost + ":" + port + "/" + db,
                    user, passwd
            );
        } catch (NumberFormatException | ClassNotFoundException | SQLException ex) {
            String msg = "MySQL connection is not created. The dburl, username , password is not correct";
            throw new Exception(msg, ex);
        }
    }

    public void manageAnalyzer() throws IOException, SQLException, ParseException {

        //  processRevisitFeatures();
        processCurrent();

    }

    private List<Analyzer> createAnalyzer() throws SQLException {

        List<Analyzer> allAnalyzers = new ArrayList<Analyzer>() {
            {
                add(new OnlineUserStatus(sqlConnection));
                add(new MethodCount(sqlConnection));
                add(new ActivityCount(sqlConnection));
                add(new UserCount(sqlConnection));
                add(new LiveViewerCount(sqlConnection));
                add(new ErrorMessageCount(sqlConnection));
                add(new LiveStreamHistory(sqlConnection));

            }
        };
        return allAnalyzers;
    }

    private void processCurrent() throws SQLException {

        HTableDescriptor[] tmpTablenNames;
        try {
            tmpTablenNames = HBaseManager.getHBaseManager().getAdmin().listTables(Tools.HBASE_TMP_TABLE_PATTERN);
            for (HTableDescriptor tmpTablenName : tmpTablenNames) {
                List<LogBean> listLogBean = new ArrayList<>();

                try {
                    listLogBean = HBaseReader.processHBaseTable(tmpTablenName);
                } catch (IOException ex) {
                    logger.error("Error1: ",ex);
                }

                try {
                    HBaseWriter.insertIntoHBase(listLogBean);
                } catch (IOException ex) {
                    logger.error("Error: "+ex);
                }

                if (listLogBean != null) {
                    HBaseLogBeanProcessor hbaseLogBeanProcessor = new HBaseLogBeanProcessor();
                    hbaseLogBeanProcessor.processLogBean(listLogBean, allAnalyzers);

                }

            }
        } catch (Exception ex) {
            logger.error("Exit: ",ex);
        }

    }

    private void processRevisitFeatures() throws SQLException, ParseException {
        Settings settings = new Settings(sqlConnection);
        Map<String, String> settingMap = settings.getSettingMap();

        String thresholedDaysStr = settingMap.get(Tools.SettingType.THRESHOLD_DAYS);
        int thresholdDays = (null == thresholedDaysStr) ? 0 : Integer.parseInt(thresholedDaysStr);

        String revisitTimeStr = settingMap.get(Tools.SettingType.REVISIT_TIME);
        String revisitFeatureStr = settingMap.get(Tools.SettingType.REVISIT_FEATURES);

        if (revisitTimeStr != null && revisitFeatureStr != null) {
            revisitTimeStr = (revisitTimeStr + "0000000000").substring(0, 10);
            long startTime = Long.parseLong(revisitTimeStr);
            long lookbackTime = getLookbackTime(revisitTimeStr, thresholdDays);
            long endTime = getCurrentDayEndTime();

            Collection<String> features = getFeatures(revisitFeatureStr);
            try {
                processArchiveReadFeatures(features, startTime, endTime, lookbackTime);
            } catch (Exception ex) {
                logger.error("", ex);
            }

        }
    }

    private long getLookbackTime(String revisitTimeStr, int thresholdDays) throws ParseException {
        DateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        Date date = sdf.parse(revisitTimeStr);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DATE, -thresholdDays);
        return Long.parseLong(sdf.format(cal.getTime()));
    }

    private long getCurrentDayEndTime() {
        DateFormat sdf = new SimpleDateFormat("yyyyyMMdd");
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 1);
        return Long.parseLong(sdf.format(cal.getTime()) + "00");
    }

    private Collection<String> getFeatures(String revisitFeatureStr) {
        Collection<String> list = new ArrayList<>();
        String[] array = revisitFeatureStr.split(Tools.FEATURE_SEPARTOR);
        list.addAll(Arrays.asList(array));
        return list;
    }

    private void processArchiveReadFeatures(Collection<String> features, long startTime, long endTime, long lookbackTime) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
