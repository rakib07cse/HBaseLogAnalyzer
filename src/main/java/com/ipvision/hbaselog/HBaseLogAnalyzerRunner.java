package com.ipvision.hbaselog;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author rakib
 */
public class HBaseLogAnalyzerRunner extends Thread {

    private static final Logger logger = LogManager.getLogger(HBaseLogAnalyzerRunner.class);

    private final String configFilePath;
    static final long SECOND = 1000L;
    static final long MINUTE = 60 * SECOND;
    static final long HOUR = 60 * MINUTE;

    public HBaseLogAnalyzerRunner(String configFilePath) {
        this.configFilePath = configFilePath;
    }

    @Override
    public void run() {
        while (true) {

            long currentTime = System.currentTimeMillis();
            long nextRunTime = (((currentTime / HOUR) + 1) * HOUR) + (60 * MINUTE);

            //***************METHOD EXTRACTOR RUNNING CODE BEGINS HERE *************************
            try {
                HBaseAnalyzerManager hbaseAnalyzerManager = new HBaseAnalyzerManager(configFilePath);
                hbaseAnalyzerManager.manageAnalyzer();
               // HBaseReader hbaseReader = new HBaseReader();
                //hbaseReader.tableReader();
            } catch (Exception ex) {
                logger.warn("Error while process method.", ex);
            }

            long waitngTime = nextRunTime - System.currentTimeMillis();

            if (waitngTime > 0) {
                try {
                    sleep(waitngTime);
                } catch (InterruptedException ex) {
                    logger.error("Exception occured for Thread.sleep()", ex);
                }
            }

        }
    }

    public static void main(String[] args) {

        HBaseLogAnalyzerRunner runner = new HBaseLogAnalyzerRunner("config.properties");
        runner.start();
    }
}
