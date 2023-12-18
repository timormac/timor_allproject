import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Title: A2_MysqlApi
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 15:23
 * @Version:1.0
 */
public class A2_MysqlApi {

    public Connection getConnection() throws SQLException, IOException {
        InputStream resource= this.getClass().getClassLoader().getResourceAsStream("jdbc.properties");
        Properties properties = new Properties();
        properties.load(resource);
        String url=properties.getProperty("url");
        String user=properties.getProperty("user") ;
        String password=properties.getProperty("password");
        String database=properties.getProperty("database");
        //设置字符集
        url +=  database +  "?useUnicode=true&characterEncoding=utf8";
        Connection connection = DriverManager.getConnection(url, user, password);

        return  connection;

    }

}
