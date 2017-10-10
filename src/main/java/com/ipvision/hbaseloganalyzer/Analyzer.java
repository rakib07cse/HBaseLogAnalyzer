/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer;

import com.ipvision.analyzer.hbase.LogBean;
import java.io.Closeable;
import java.sql.SQLException;
import java.util.List;

/**
 *
 * @author rakib
 */
public interface Analyzer extends Closeable {

    public void clear();

    public void processLog(List<LogBean> listLogBean);

    public void saveToDB() throws SQLException;
    public void recalculate(long startTime, long endTime) throws SQLException;

    public void deleteFromDB(long startTime, long endTime) throws SQLException;
    
}
