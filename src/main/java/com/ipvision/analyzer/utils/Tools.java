/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ipvision.analyzer.utils;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author rakib
 */
public class Tools {

    public static final int SQL_BATCH_LIMIT = 300;

    public static final String LOG_DIR_KEY = "logdir";
    public static final String PROCESSED_LOG_MAP = "processedlogmap";
    public static final String HBASE_TABLE_NAME = "methodData";
    public static final String HBASE_TABLE_COLUME_FAMILY_NAME = "method";
    public static final String HBASE_TABLE_FIRST_COLUME_NAME = "method_name";
    public static final String HBASE_TABLE_SECOND_COLUME_NAME = "method_data";
    public static final String HBASE_TMP_TABLE_PATTERN = ".*_tmp";

    public static final String MYSQL_HOST_KEY = "mysql.host";
    public static final String MYSQL_PORT_KEY = "mysql.port";
    public static final String MYSQL_DB_KEY = "mysql.db";
    public static final String MYSQL_USER_KEY = "mysql.user";
    public static final String MYSQL_PASSWD_KEY = "mysql.passwd";
    
    public static final String FEATURE_SEPARTOR = ",";

    public static final String UUID_PATTERN = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";
    
      public class SettingType {

        public static final String THRESHOLD_DAYS = "threshold_days";
        public static final String REVISIT_TIME = "revisit_time";
        public static final String REVISIT_FEATURES = "revisit_features";
    }

}
