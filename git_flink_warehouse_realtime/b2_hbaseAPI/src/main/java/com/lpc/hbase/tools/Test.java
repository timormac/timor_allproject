package com.lpc.hbase.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @Title: Test
 * @Package: com.lpc.hbase.tools
 * @Description:
 * @Author: lpc
 * @Date: 2023/11/22 15:11
 * @Version:1.0
 */
public class Test {

    public static void main(String[] args) throws Exception {


        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "project1,project2,project3");
        Connection connection = ConnectionFactory.createConnection(config);
        HbaseTools.createTable(connection,"spu");


// 使用连接进行HBase操作


    }
}
