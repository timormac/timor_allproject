package com.lpc.datamock.tools;

import com.lpc.datamock.dao.Dao;
import com.lpc.datamock.dao.Spu;
import org.junit.Test;

import java.io.DataOutput;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;

/**
 * @Title: A2_TableQueryDao
 * @Package: com.lpc.datamock.tools
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/23 20:42
 * @Version:1.0
 */
public class A2_TableQueryDao {

    Connection connection;
    Statement statement;


    public A2_TableQueryDao(Connection connection) {
        this.connection = connection;
    }


    public ArrayList<Dao> DaoQueryset(String tb, String clsName, String filterCondition) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        String sql = "select  * from " + tb +  " "   + filterCondition;
        this.statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);

        ArrayList<Dao> dao_arr = new ArrayList<>();

        //String className = "com.lpc.datamock.dao.Spu";
        Class<?> clazz = Class.forName(clsName);

        //获取属性个数
        int length = clazz.getDeclaredFields().length;

        while ( resultSet.next()){
            ArrayList<String> arr = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                String str = resultSet.getString(i + 1);
                arr.add(str);
            }
            Object object = clazz.newInstance();
            Dao dao = (Dao) object ;
            dao.getInstance(arr);
            dao_arr.add(dao);
        }

        return dao_arr;
    }

    public ArrayList<Dao> DaoQueryset(String tb,String clsName) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        return  DaoQueryset(tb,clsName,"");

    }

    public void close() throws SQLException {
        this.statement.close();
        this.connection.close();

    }




    public ArrayList<Dao> DaoQuerysetDeveloping() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        String sql = "select  * from spu limit 2" ;
        this.statement = this.connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);


        ArrayList<Dao> dao_arr = new ArrayList<>();

        String clsName = "com.lpc.datamock.dao.Spu";
        Class<?> clazz = Class.forName(clsName);
        Field[] fields = clazz.getDeclaredFields();


        while( resultSet.next()){
            Object object = clazz.newInstance();

            for (int i=0;i<fields.length;i++) {
                Class<?> type = fields[i].getType();
                Object colume = resultSet.getObject(i + 1, type);
                fields[i].set(object,colume);
            }

            dao_arr.add(  (Dao)object );
        }

        return dao_arr;

    }


}
