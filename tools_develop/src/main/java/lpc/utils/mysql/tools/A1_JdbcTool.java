package lpc.utils.mysql.tools;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * @Title: A1_JdbcTool
 * @Package: lpc.utils.mysql.tools
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/19 11:22
 * @Version:1.0
 */
public class A1_JdbcTool {

    private  DruidDataSource druidDataSource ;
    private  String  driver;
    private  String  url;
    private  String  user;
    private  String  password;



    public A1_JdbcTool( String driver, String url, String user, String password) {
        this.druidDataSource = druidDataSource;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;


        druidDataSource = new DruidDataSource();
        druidDataSource.setDriverClassName(driver);
        druidDataSource.setUrl(url);
        druidDataSource.setUsername(user);
        druidDataSource.setPassword(password);

        // 设置初始化连接池时池中连接的数量
        druidDataSource.setInitialSize(5);
        // 设置同时活跃的最大连接数
        druidDataSource.setMaxActive(10);
        // 设置空闲时的最小连接数，必须介于 0 和最大连接数之间，默认为 0
        druidDataSource.setMinIdle(1);
        // 设置没有空余连接时的等待时间，超时抛出异常，-1 表示一直等待
        druidDataSource.setMaxWait(-1);
        // 验证连接是否可用使用的 SQL 语句
        druidDataSource.setValidationQuery("select 1");
        // 指明连接是否被空闲连接回收器（如果有）进行检验，如果检测失败，则连接将被从池中去除
        // 注意，默认值为 true，如果没有设置 validationQuery，则报错
        // testWhileIdle is true, validationQuery not set
        druidDataSource.setTestWhileIdle(true);
        // 借出连接时，是否测试，设置为 false，不测试，否则很影响性能
        druidDataSource.setTestOnBorrow(false);
        // 归还连接时，是否测试
        druidDataSource.setTestOnReturn(false);
        // 设置空闲连接回收器每隔 30s 运行一次
        druidDataSource.setTimeBetweenEvictionRunsMillis(30 * 1000L);
        // 设置池中连接空闲 30min 被回收，默认值即为 30 min
        druidDataSource.setMinEvictableIdleTimeMillis(30 * 60 * 1000L);

        System.out.println("Druid线程池初始化");
    }


    //  默认是msyq,读取配置文件
    public A1_JdbcTool() {
        this(
                A1_ConfigProperty.MYSQL_DRIVER,
                A1_ConfigProperty.MYSQL_URL,
                A1_ConfigProperty.MYSQL_USER,
                A1_ConfigProperty.MYSQL_PASSWORD
        );

    }

    public  Connection getPoolConnect() throws SQLException {
        System.out.println("获取mysql连接");
        return druidDataSource.getConnection();

    }

    public  HashMap< String, String > getTableColume(Connection connect, String tableName) throws SQLException {
        LinkedHashMap<String, String > map = new LinkedHashMap<>();
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




}
