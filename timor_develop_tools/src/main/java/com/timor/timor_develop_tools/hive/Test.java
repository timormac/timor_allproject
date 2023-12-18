package com.timor.timor_develop_tools.hive;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Title: Test
 * @Package: com.timor.timor_develop_tools.hive
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/2 18:54
 * @Version:1.0
 */
public class Test {
    public static void main(String[] args) throws Exception {

        MysqlTableSyncHive sync = new MysqlTableSyncHive();
        sync.hiveCreateTableBatch("ods_mysql","/user/hive/warehouse");

//        sync.hiveCreateTable("orderinfo");

    }

}
