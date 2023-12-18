package com.lpc.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @Title: DML
 * @Package: com.lpc.hbase.demo
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/23 12:04
 * @Version:1.0
 */
public class DML {
    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","project1,project2,project3");
        //获取conncetion
        Connection connection = ConnectionFactory.createConnection(config);
        //获取表连接
        Table table = connection.getTable(TableName.valueOf("dev", "tb1"));
        //创建put指定rowkey
        Put put = new Put(Bytes.toBytes("rowkey1"));
        //指定列族，字段名，值
        put.addColumn(Bytes.toBytes("cols1") ,Bytes.toBytes("name"),Bytes.toBytes("name1"));
        //放入数据
        table.put(put);
        System.out.println("放入数据1");

        Put put2 = new Put(Bytes.toBytes("rowkey2"));
        put2.addColumn(Bytes.toBytes("cols1") ,Bytes.toBytes("age"),Bytes.toBytes("age2"));
        table.put(put2);

        //单数据查询，输入rk
        Get get = new Get(Bytes.toBytes("rowkey1"));
        //数据查询的列族和字段
       // get.addColumn(Bytes.toBytes("cols1"), Bytes.toBytes("name"));
        Result result = table.get(get);

        //解析result返回结果,1个rk可能返回多个列族和多个字段
        for (Cell cell : result.rawCells()) {
            //获取列族
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            //获取列名
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            //获取值
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //scan扫描表
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result1 : results) {

            //只要name字段
            List<Cell> namecells = result1.getColumnCells(Bytes.toBytes("cols1"), Bytes.toBytes("name"));
            for (Cell cell : namecells) {
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
        connection.close();


    }
}
