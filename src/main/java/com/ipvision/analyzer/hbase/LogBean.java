/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.analyzer.hbase;

import java.io.Serializable;

/**
 *
 * @author rakib
 */
public class LogBean implements Serializable {

    private String timeStamp;
    private String method;
    private String data;

    public LogBean() {
    }

    public LogBean(String timeStamp, String method, String data) {

        this.timeStamp = timeStamp;
        this.method = method;
        this.data = data;
    }

    public void setTimestmap(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getTimestamp() {
        return timeStamp;

    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getMethod() {
        return method;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getData() {
        return data;
    }
}
