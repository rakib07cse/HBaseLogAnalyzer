/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.hbaseloganalyzer.test;


import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 *
 * @author rakib
 */
public class HBaseLogAnalyzerTestRunner {
    
    public static void main(String[] args){
       Result result = JUnitCore.runClasses(HBaseLogAnalyzerTest.class);
       for(Failure failure: result.getFailures()){
           System.out.println("Print Here: "+failure.toString());
       }
    }
    
}
