/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.analyzer.hbase;

import com.ipvision.hbaseloganalyzer.Analyzer;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rakib
 */
public class HBaseLogBeanProcessor implements Runnable {

    private List<LogBean> listLogBean = null;
    private Thread analyzerThread;
    private Analyzer analyzer;

    public HBaseLogBeanProcessor(Analyzer analyzer, List<LogBean> listLogBean) {
        this.analyzer = analyzer;
        this.listLogBean = listLogBean;
    }

    public HBaseLogBeanProcessor() {
    }

    public void processLogBean(List<LogBean> listLogBean, List<Analyzer> allAnalyzers) throws SQLException {

        HBaseLogBeanProcessor[] hbaseLogBeanProcessor = new HBaseLogBeanProcessor[allAnalyzers.size()];
        int countAnalyzer = 0;
        for (Analyzer analyzer : allAnalyzers) {
            hbaseLogBeanProcessor[countAnalyzer] = new HBaseLogBeanProcessor(analyzer, listLogBean);
            hbaseLogBeanProcessor[countAnalyzer].start();

            countAnalyzer++;

        }

    }

    @Override
    public void run() {
        analyzer.processLog(listLogBean);
        try {
            analyzer.saveToDB();
        } catch (SQLException ex) {
            Logger.getLogger(HBaseReader.class.getName()).log(Level.SEVERE, null, ex);
        }
        analyzer.clear();
    }

    private void start() {
        if (analyzerThread == null) {
            analyzerThread = new Thread(this);
            analyzerThread.start();
        }
    }

}
