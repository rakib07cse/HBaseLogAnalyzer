/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer.test;

import com.ipvision.analyzer.hbase.HBaseManager;
import com.ipvision.analyzer.hbase.HBaseReader;
import com.ipvision.analyzer.hbase.LogBean;
import com.ipvision.analyzer.utils.Tools;
import com.ipvision.hbaselog.HBaseAnalyzerManager;
import com.ipvision.hbaseloganalyzer.OnlineUserStatus;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author rakib
 */
public class OnlineUserStatusTest {

    private static List<LogBean> listLogBean;
    private final Connection sqlConnection;
    private final String fileName = "config.properties";

    public OnlineUserStatusTest() throws Exception {

        HBaseAnalyzerManager manager = new HBaseAnalyzerManager(fileName);
        sqlConnection = manager.getConnection();
    }

    @BeforeClass
    public static void setUpClass() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HTableDescriptor[] tmpTableNames;
        tmpTableNames = HBaseManager.getHBaseManager().getAdmin().listTables(Tools.HBASE_TMP_TABLE_PATTERN);
        for (HTableDescriptor tmpTableName : tmpTableNames) {
            if (HBaseManager.getHBaseManager().getAdmin().isTableEnabled(tmpTableName.getNameAsString())) {
                listLogBean = HBaseReader.processHBaseTable(tmpTableName);
   
            }
        }
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
    //
    // @Test
    // public void hello() {}
    // Check analytics_user_online_status
    @Test
    public void testProcessLog() throws SQLException {

        OnlineUserStatus userStatus = new OnlineUserStatus(sqlConnection);
        userStatus.processLog(listLogBean);
        userStatus.saveToDB();
      
    }
}
