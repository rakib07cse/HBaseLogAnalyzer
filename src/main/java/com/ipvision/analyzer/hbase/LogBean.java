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


    private String timestamp;
    private String logLevel;
    private String eventType;
    private String requestId;
    private String liveStreamHistroy;
    private String liveStreamParams;
    private String methodName;
    private String params;

    public LogBean() {

    }

    public LogBean(String timestamp, String logLevel, String eventType, String requestId, String liveStreamHistory, String liveStreamParams, String methodName, String params) {
        this.timestamp = timestamp;
        this.logLevel = logLevel;
        this.eventType = eventType;
        this.requestId = requestId;
        this.liveStreamHistroy = liveStreamHistory;
        this.liveStreamParams = liveStreamParams;
        this.methodName = methodName;
        this.params = params;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setLiveStreamHistory(String liveStreamHistory) {
        this.liveStreamHistroy = liveStreamHistory;
    }

    public String getLiveStreamHistory() {
        return liveStreamHistroy;
    }

    public void setLiveStreamParams(String liveStreamParams) {
        this.liveStreamParams = liveStreamParams;
    }

    public String getLiveStreamParams() {
        return liveStreamParams;
    }

    public void setMethodName(String methodName) {
        if (methodName != null && !methodName.isEmpty()) {
            this.methodName = methodName;
        } 
//        else {
//            this.methodName = "noMethod";
//        }
    }

    public String getMethodName() {
        return methodName;
    }

    public void setParams(String params) {
        if (params != null && !params.isEmpty()) {
         //   this.params = "{" + params + "}";
          this.params = params;
        }
//        } else {
//            this.params = "{" + "" + "}";
//        }
    }

    public String getParams() {
        return params;
    }

}
