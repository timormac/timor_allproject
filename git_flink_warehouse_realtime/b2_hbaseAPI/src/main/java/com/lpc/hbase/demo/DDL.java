package com.lpc.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Title: DDL
 * @Package: com.lpc.hbase.demo
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/23 12:03
 * @Version:1.0
 */
public class DDL {
    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","project1,project2,project3");

        //获取conncetion
        Connection connection = ConnectionFactory.createConnection(config);
        //获取admin
        Admin admin = connection.getAdmin();


        //查看表是否存在,也可以只传一个表名,默认是default库
        boolean tb1 = admin.tableExists( TableName.valueOf("dev","tb1"));
        System.out.println( "表是否存在"+tb1);

        //创建表
        TableDescriptorBuilder tb_builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("dev","mactest"));
        //创建2个列族
        ColumnFamilyDescriptorBuilder col1_builder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cols1"));
        ColumnFamilyDescriptorBuilder col2_builder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cols2"));
        //放入2个列族
        tb_builder.setColumnFamily(col1_builder.build());
        tb_builder.setColumnFamily(col2_builder.build());
        //执行建表
        admin.createTable(tb_builder.build());

        admin.close();
        connection.close();

    }
}
