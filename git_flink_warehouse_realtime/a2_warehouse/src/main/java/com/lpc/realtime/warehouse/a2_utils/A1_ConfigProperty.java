package com.lpc.realtime.warehouse.a2_utils;

/**
 * @Author Timor
 * @Date 2023/11/2 18:52
 * @Version 1.0
 */
public class A1_ConfigProperty {

    public  static  final  String  HBASE_SERVER ="project1,project2,project3";
    public  static  final  String  KAFKA_SERVER = "project1:9092,project2:9092,project3:9092";
    public  static  final  String  ZOOKEEPER_SERVER = "project1,project2,project3";
    public static final String HDFS_PATH  = "hdfs://project1:8020";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    public  static  final  String  PHOENIX_URL = "jdbc:phoenix:project1,project2,project3:2181";


    //mysql
    public  static  final  String  MYSQL_URL = "jdbc:mysql://project1:3306/flink_warehouse_db?useUnicode=true&characterEncoding=utf8";
    public  static  final  String  MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public  static  final  String  MYSQL_USER = "root";
    public  static  final  String  MYSQL_PASSWORD = "121995";

}
