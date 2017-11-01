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
import com.ipvision.hbaseloganalyzer.MethodCount;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTable;
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
public class MethodCountTest {

    private static List<LogBean> listLogBean;
    private static final String fileName = "config.properties";
    private final Connection sqlConnection;

    public MethodCountTest() throws Exception {
        HBaseAnalyzerManager manager = new HBaseAnalyzerManager(fileName);
        sqlConnection = manager.getConnection();
    }

    @BeforeClass
    public static void setUpClass() throws MasterNotRunningException, ZooKeeperConnectionException, IOException, Exception {
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
    //Check analytics_method_count
    @Test
    public void testProcessLog() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {

        MethodCount methodCount = new MethodCount(sqlConnection);
        double procesStartTime = System.currentTimeMillis();
        methodCount.processLog(listLogBean);
        double procesEnd_SaveStartTime = System.currentTimeMillis();
        methodCount.saveToDB();
        double saveEndime = System.currentTimeMillis();

        System.out.println("Process time in second: " + (procesEnd_SaveStartTime - procesStartTime) / 10000 + "  DB Save time in sec: " + (saveEndime - procesEnd_SaveStartTime) / 1000);

    }
}
