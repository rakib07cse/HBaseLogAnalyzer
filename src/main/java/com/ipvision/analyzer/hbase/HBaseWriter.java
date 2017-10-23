/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.analyzer.hbase;

import com.ipvision.analyzer.utils.Tools;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author rakib
 */
public class HBaseWriter {
    
    private static final Logger hBaseInserterLogger = LoggerFactory.getLogger(HBaseWriter.class);
    private static final SimpleDateFormat tableNameFormat = new SimpleDateFormat("yyyyMM");
    
    public static void insertIntoHBase(List<LogBean> logBeanList) throws IOException {
        long timeStamp = System.currentTimeMillis();
        String methodTable = tableNameFormat.format(timeStamp) + "_method";
        String liveStreamTable = tableNameFormat.format(timeStamp) + "_liveStream";
        String errorTable = tableNameFormat.format(timeStamp) + "_error";
        
        if (!HBaseManager.getHBaseManager().getAdmin().tableExists(methodTable)) {
            HBaseManager.getHBaseManager().createHBaseTable(methodTable);
        }
        if (!HBaseManager.getHBaseManager().getAdmin().tableExists(liveStreamTable)) {
            HBaseManager.getHBaseManager().createHBaseTable(liveStreamTable);
        }
        if (!HBaseManager.getHBaseManager().getAdmin().tableExists(errorTable)) {
            HBaseManager.getHBaseManager().createHBaseTable(errorTable);
        }
        
        try {
            HTable methodHTable = HBaseManager.getHBaseManager().createHTable(methodTable);
            HTable liveStreamHTable = HBaseManager.getHBaseManager().createHTable(liveStreamTable);
            HTable errorHTable = HBaseManager.getHBaseManager().createHTable(errorTable);
            
            List<Put> methodBeanPutList = new ArrayList<>();
            List<Put> liveStreamBeanPutList = new ArrayList<>();
            List<Put> errorBeanPutList = new ArrayList<>();
            
            for (LogBean logBean : logBeanList) {
                
                if (logBean.getTimestamp() != null) {
                    if (logBean.getLiveStreamHistory() != null) {
                        
                        Put liveStreamPut = HBaseManager.getHBaseManager().createLoggerPut(logBean.getTimestamp());
                        if (logBean.getLiveStreamHistory() != null) {
                            liveStreamPut.add(Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_FIRST_COLUME_NAME), Bytes.toBytes(logBean.getLiveStreamHistory()));
                        }
                        if (logBean.getLiveStreamParams() != null) {
                            liveStreamPut.add(Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_LIVESTREAMHISTORY_SECOND_COLUME_NAME), Bytes.toBytes(logBean.getLiveStreamParams()));
                        }
                        liveStreamBeanPutList.add(liveStreamPut);
                        
                    } else if ("ERROR".equals(logBean.getLogLevel()) || "WARN".equals(logBean.getLogLevel()) || "FATAL".equals(logBean.getLogLevel())) {
                        Put errorPut = HBaseManager.getHBaseManager().createLoggerPut(logBean.getTimestamp());
                        if (logBean.getLogLevel() != null) {
                            errorPut.add(Bytes.toBytes(Tools.HBASE_TABLE_NOTINFO_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_NOTINOF_FIRST_COLUME_NAME), Bytes.toBytes(logBean.getLogLevel()));
                        }
                        if (logBean.getEventType() != null) {
                            errorPut.add(Bytes.toBytes(Tools.HBASE_TABLE_NOTINFO_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_NOTINFO_SECOND_COLUME_NAME), Bytes.toBytes(logBean.getEventType()));
                        }
                        errorBeanPutList.add(errorPut);
                    } else {
                        Put methodPut = HBaseManager.getHBaseManager().createLoggerPut(logBean.getTimestamp());
                        if (logBean.getMethodName() != null) {
                            methodPut.add(Bytes.toBytes(Tools.HBASE_TABLE_METHOD_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_METHOD_FIRST_COLUME_NAME), Bytes.toBytes(logBean.getMethodName()));
                        }
                        if (logBean.getParams() != null) {
                            methodPut.add(Bytes.toBytes(Tools.HBASE_TABLE_METHOD_COLUME_FAMILY_NAME), Bytes.toBytes(Tools.HBASE_TABLE_METHOD_SECOND_COLUME_NAME), Bytes.toBytes(logBean.getParams()));
                        }
                        methodBeanPutList.add(methodPut);
                    }
                    
                }
            }
            
            methodHTable.put(methodBeanPutList);
            liveStreamHTable.put(liveStreamBeanPutList);
            errorHTable.put(errorBeanPutList);
            
            if (methodHTable != null) {
                methodHTable.flushCommits();
            }
            if (liveStreamHTable != null) {
                liveStreamHTable.flushCommits();
            }
            if (errorHTable != null) {
                errorHTable.flushCommits();
            }
            String msg = "Data insert successfully";
            hBaseInserterLogger.info(msg);
        } catch (Exception ex) {
            String msg = "";
            ex.printStackTrace();
            hBaseInserterLogger.error(msg);
        }
        
    }
    
}
