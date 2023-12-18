import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.mysql.jdbc.Connection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Title: A3_ConnectPool
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 15:28
 * @Version:1.0
 */
public class A3_ConnectPool {
    DruidDataSource source;

    public A3_ConnectPool()  throws IOException {
        this.source= new DruidDataSource();
        Properties property = getProperty();

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



    DruidPooledConnection  getPoolConnect() throws SQLException {
        return  this.source.getConnection();
    }



     Properties  getProperty() throws IOException {

        InputStream resource= this.getClass().getClassLoader().getResourceAsStream("jdbc.properties");
        Properties properties = new Properties();
        properties.load(resource);
        return  properties;
    }

}
