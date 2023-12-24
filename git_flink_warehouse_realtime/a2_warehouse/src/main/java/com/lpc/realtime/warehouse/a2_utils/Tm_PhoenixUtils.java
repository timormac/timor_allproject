package com.lpc.realtime.warehouse.a2_utils;

import com.google.inject.internal.util.$Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Timor
 * @Date 2023/12/22 20:39
 * @Version 1.0
 */
public class Tm_PhoenixUtils {

    public  static void insertHbaseTable(Connection connect, String dataBase, String tableName, Map<String,Object> columnMap) throws SQLException {
       String  sql =  "upsert into " + dataBase + "." + tableName + "(" ;
        int size = columnMap.keySet().size();

        String sql2 = " values(" ;
        int flag = 1;
        for ( String key : columnMap.keySet() ) {
            if(flag < size ){
               sql += key + ",";
               sql2 +="?,";
            }else {
                sql += key + ")" ;
                sql2 +="?)";
            }
            flag +=1;
        }

        sql += sql2;

        System.out.println(sql);

        PreparedStatement preparedStatement = connect.prepareStatement(sql) ;
        Tm_MysqlUtils.prepareSqlSet(preparedStatement,columnMap);
        preparedStatement.execute();
        preparedStatement.close();
        connect.commit();
        System.out.println("插入一条数据:" +columnMap );
    }


    public static void createHbaseTable(Connection connect, String dataBase , String tableName, Map<String,Object> columnMap, String rowKey ){

        String sql = " create table if not exists  "  + dataBase + "." + tableName + "( "   ;

        for (String key : columnMap.keySet()) {
            if( key.equals(rowKey) ){
                sql += "\n" + rowKey +  "  " + columnMap.get(key) + "  primary key" ;
            }else {
                sql += ", \n " + key + "  " + columnMap.get(key);
            }
        }
        sql += ")";

        System.out.println(sql);
        PreparedStatement preparedStatement = null;

        try{
            preparedStatement = connect.prepareStatement(sql);
            boolean execute = preparedStatement.execute();
        }catch (Exception e){
            System.out.println( "创建失败" + e);

        }finally {
            try{
                preparedStatement.close();
            }catch (Exception e){
                System.out.println("关闭失败"+ e);
            }
        }
    }


}
