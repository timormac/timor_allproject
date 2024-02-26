package com.lpc.datamock.tools;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;


/**
 * @Title: A1_JDBCPool
 * @Package: tools
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 16:09
 * @Version:1.0
 */
public class A1_JDBCPool {

    private DruidDataSource source;

    private static  A1_JDBCPool  pool;

    static {
        try {
            pool = new A1_JDBCPool();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private A1_JDBCPool()  throws IOException {
        Properties property = new A0_PropertyUtils("jdbc.properties").getProperty();
        this.source= new DruidDataSource();
        source.setDriverClassName(property.getProperty("driver"));
        source.setUrl(property.getProperty("url"));
        source.setUsername(property.getProperty("user"));
        source.setPassword(property.getProperty("password"));
        //一开始提前申请好5个连接，不够了，重写申请
        source.setInitialSize( Integer.parseInt(property.getProperty("initialSize"))  );
        //最多不超过10个，如果10都用完了，还没还回来，就会出现等待
        source.setMaxActive(Integer.parseInt(property.getProperty("MaxActive")) );
        //用户最多等1000毫秒，如果1000毫秒还没有人还回来，就异常了
        source.setMaxWait(Integer.parseInt(property.getProperty("MaxWait"))  );
    }

    public DruidDataSource getSource() {
        return source;
    }

    public static DruidPooledConnection  getPoolConnect() throws SQLException {

        System.out.println( "获取了一次连结池子");
        return  pool.getSource().getConnection();
    }


}
