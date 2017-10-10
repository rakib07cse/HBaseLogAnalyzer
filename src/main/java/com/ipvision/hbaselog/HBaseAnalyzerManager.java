/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaselog;

import com.ipvision.analyzer.hbase.HBaseReader;
import com.ipvision.analyzer.utils.Tools;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 *
 * @author rakib
 */
public class HBaseAnalyzerManager {

    private final Connection sqlConnection;

    public HBaseAnalyzerManager(String configFilepath) throws Exception {
        Properties properties = loadProperties(configFilepath);

        sqlConnection = createSqlConnection(properties);
//        sqlConnection.setAutoCommit(false);
//        sqlConnection.rollback();
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

    public void manageAnalyzer() throws IOException, SQLException {
        HBaseReader hbaseReader = new HBaseReader();
        hbaseReader.processHBaseTable(sqlConnection);
    }
}
