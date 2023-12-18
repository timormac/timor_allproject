package com.lpc.datamock.tools;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.lpc.datamock.dao.Dao;
import com.lpc.datamock.dao.Spu;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @Title: Test
 * @Package: com.lpc.datamock.tools
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/23 20:59
 * @Version:1.0
 */
public class Test {

    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        String className = "com.lpc.datamock.dao.Spu";

        Class<?> clazz = Class.forName(className);

        Object o = func1(clazz);


    }



   public static  <T> T func1( Class<T> type ) throws IllegalAccessException, InstantiationException {
       Object object = type.newInstance();
       return  (T)object;

   }
}
