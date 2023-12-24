package com.lpc.realtime.warehouse.a2_utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author Timor
 * @Date 2023/12/22 16:27
 * @Version 1.0
 */
public class Tm_MysqlUtils {


    public static HashMap<String, Object> getTableColume(Connection connect, String tableName) throws SQLException {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        String sql = "describe  " + tableName ;
        PreparedStatement preparedStatement = connect.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while ( resultSet.next() ) {
            String field = resultSet.getString("field");
            String type = (resultSet.getString("type").split("\\("))[0];
            map.put(field,type);
        }
        preparedStatement.close();
        return  map;
    }


    public static HashMap<String, Object> getTableColumeForPhoenix(Connection connect, String tableName) throws SQLException {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        String sql = "describe  " + tableName ;
        PreparedStatement preparedStatement = connect.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        while ( resultSet.next() ) {
            String field = resultSet.getString("field");
            String type = (resultSet.getString("type").split("\\("))[0];

            switch ( (String) type){
                case "datetime":
                    type = "varchar";
                    break;
                case "date":
                    type = "varchar";
                    break;
            }
            map.put(field,type);
        }
        preparedStatement.close();
        return  map;
    }

    public static void prepareSqlSet(PreparedStatement pst,Map<String,Object> map ) throws SQLException {
        int flag = 1;
        for (Object value : map.values()) {
            pst.setObject(flag,value );
            flag += 1;
        }
    }


}
