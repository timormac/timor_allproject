package timor.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import timor.utils.A0_Config;

import java.sql.SQLException;


/**
 * @Author Timor
 * @Date 2023/12/19 15:03
 * @Version 1.0
 */
public class A0_ConnectPool {

    private  static DruidDataSource source;

    static {
        source= new DruidDataSource();
        source.setDriverClassName(A0_Config.MYSQL_DRIVER);
        source.setUrl(A0_Config.MYSQL_URL);
        source.setUsername(A0_Config.MYSQL_USER);
        source.setPassword(A0_Config.MYSQL_PASSWORD);
        //一开始提前申请好5个连接，不够了，重写申请
        source.setInitialSize( 5 );
        //最多不超过10个，如果10都用完了，还没还回来，就会出现等待
        source.setMaxActive(10 );
        //用户最多等1000毫秒，如果1000毫秒还没有人还回来，就异常了
        source.setMaxWait(1000);
    }

    public static DruidPooledConnection getPoolConnect() throws SQLException {
        return  source.getConnection();
    }


}
