package a2_utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.lpc.realtime.warehouse.a2_utils.Tm_MysqlConnectPool;
import com.lpc.realtime.warehouse.a2_utils.Tm_MysqlUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @Author Timor
 * @Date 2023/12/22 16:27
 * @Version 1.0
 */
public class Tm_MysqlUtils_Test {

    DruidDataSource druidDataSource;
    Connection  connect ;

    @Before
    public void setup() throws SQLException {
        druidDataSource = Tm_MysqlConnectPool.getDruidDataSource();
        connect = druidDataSource.getConnection();
    }

    @Test
    public void getTableColume() throws SQLException {
        HashMap<String, Object> orderinfo = Tm_MysqlUtils.getTableColume(connect, "orderinfo");
        System.out.println( orderinfo );
    }


    @After
    public void close() throws SQLException {
        connect.close();
        druidDataSource.close();
    }
}
