package com.lpc.hbase.tools;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * @Author Timor
 * @Date 2023/11/23 17:45
 * @Version 1.0
 */
public class HbaseTools {

    public static  Connection getHbaseConnection(String servers) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum",servers);
        //获取conncetion
        Connection connection = ConnectionFactory.createConnection(config);
        return  connection;
    }



    public static void insertData(Connection connection,String tableName, String columName,String rowkey,String value, String familyName,String spaceName) throws IOException {

        Table table = connection.getTable(TableName.valueOf(spaceName, tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columName),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public static void insertData(Connection connection,String tableName, String columName,String rowkey,String value) throws IOException {
        insertData(connection,tableName,columName,rowkey,value,"main","dev");

    }


    public static void createTable(Connection connection,String tableName  , String columnFamily,  String spacename ) throws IOException {
        Admin admin = connection.getAdmin();
        boolean  tbExists = admin.tableExists(TableName.valueOf( spacename+":"+tableName));
        if(tbExists) System.out.println( "表已存在");
        else{
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(spacename, tableName));
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));

            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.createTable(tableDescriptorBuilder.build());
            System.out.println("建表"+tableName);
        }
        admin.close();
    }

    public static void createTable(Connection connection, String tableName) throws IOException {
        createTable(connection,tableName,"main","dev");
    }



}
