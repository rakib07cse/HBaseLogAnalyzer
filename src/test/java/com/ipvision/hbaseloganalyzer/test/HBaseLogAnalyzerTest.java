/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer.test;

import com.ipvision.analyzer.hbase.HBaseManager;

import com.ipvision.analyzer.utils.Tools;
import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 *
 * @author rakib
 */
public class HBaseLogAnalyzerTest {
    
  
   private static Logger logger = Logger.getLogger(HBaseLogAnalyzerTest.class);
   
    @Test
    public void hbaseReadTable(){
      // List<LogBean> listLogBean = HBaseReader.processHBaseTable(HTableDescriptor.META_TABLEDESC)
      HTableDescriptor[] tmpTablenNames;
        try {
            tmpTablenNames = HBaseManager.getHBaseManager().getAdmin().listTables(Tools.HBASE_TMP_TABLE_PATTERN);
            for(HTableDescriptor tableName: tmpTablenNames){
                System.out.println("Table Name: "+ tableName.getNameAsString());
            }
            
        } catch (MasterNotRunningException ex) {
            logger.error("MasterNot RunningExection: ",ex);
            //Logger.getLogger(HBaseLogAnalyzerTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ZooKeeperConnectionException ex) {
            logger.error("ZooKeeperConnectionException", ex);
            //Logger.getLogger(HBaseLogAnalyzerTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            logger.error("IOExecption: ", ex);
          //  Logger.getLogger(HBaseLogAnalyzerTest.class.getName()).log(Level.SEVERE, null, ex);
        }
     
    }
    
}
