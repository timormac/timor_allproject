package a2_utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.lpc.realtime.warehouse.a2_utils.Tm_MysqlConnectPool;
import com.lpc.realtime.warehouse.a2_utils.Tm_MysqlUtils;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixConnectPool;
import com.lpc.realtime.warehouse.a2_utils.Tm_PhoenixUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @Author Timor
 * @Date 2023/12/23 20:48
 * @Version 1.0
 */
public class Tm_PhoenixUtils_Test {

    DruidDataSource mysqlSource;
    Connection mysqlConnect ;
    DruidDataSource phoenixSouce;
    Connection phoenixConnect;

    @Before
    public void setup() throws SQLException {
        mysqlSource = Tm_MysqlConnectPool.getDruidDataSource();
        mysqlConnect = mysqlSource.getConnection();
        phoenixSouce = Tm_PhoenixConnectPool.getDruidDataSource();
        phoenixConnect = phoenixSouce.getConnection();
    }

    public void createHbaseTable() throws SQLException {
        HashMap<String, Object> map = Tm_MysqlUtils.getTableColumeForPhoenix(mysqlConnect, "flinkconfig");
        Tm_PhoenixUtils.createHbaseTable(phoenixConnect,"warehouse","flinkconfig",map,"id");
    }


    @Test
    public void insertHbaseTable() throws SQLException {

        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("spu_name","asd");
        map.put("create_time","2023-12-24 12:18:14");
        map.put("merchantid",13);
        map.put("price",3.35);
        map.put("id",977);
        Tm_PhoenixUtils.insertHbaseTable(phoenixConnect,"warehouse","spu",map);

    }



    @After
    public void close() throws SQLException {
        mysqlConnect.close();
        mysqlSource.close();
        phoenixConnect.close();
        phoenixSouce.close();
    }

}
