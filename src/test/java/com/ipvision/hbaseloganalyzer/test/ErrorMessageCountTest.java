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
import com.ipvision.hbaseloganalyzer.ErrorMessageCount;
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
public class ErrorMessageCountTest {

    private static List<LogBean> listLogBean;
    private static Connection sqlConnection;
    private static final String fileName = "config.properties";

    public ErrorMessageCountTest() throws Exception {

        HBaseAnalyzerManager manager = new HBaseAnalyzerManager(fileName);
        sqlConnection = manager.getConnection();
    }

    @BeforeClass
    public static void setUpClass() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        HBaseManager.getHBaseManager().getAdmin().enableTable("2017102219_tmp");
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
    @Test
    public void testProcessLog() throws SQLException {
        ErrorMessageCount errMessageCount = new ErrorMessageCount(sqlConnection);
        double procesStartTime = System.currentTimeMillis();
        errMessageCount.processLog(listLogBean);
        double processEnd_SaveStart = System.currentTimeMillis();
        errMessageCount.saveToDB();
        double saveEndTime = System.currentTimeMillis();
        System.out.println("Process time in second: " + (processEnd_SaveStart - procesStartTime) / 10000 + "  DB Save time in sec: " + (saveEndTime - processEnd_SaveStart) / 1000);
    }

}
