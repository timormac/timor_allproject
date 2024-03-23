package tools;

import lpc.utils.mysql.dao.Mysql_Dao;
import lpc.utils.mysql.dao.Mock_OrderMysqlAbsctractMock;
import lpc.utils.mysql.tools.Mysql_Tool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @Author Timor
 * @Date 2024/2/24 20:25
 * @Version 1.0
 */
public class _MysqlToolTest {

    private Connection connect;


    @Before
    public void setUp() throws SQLException {
        connect = Mysql_Tool.getPoolConnect();
    }

    @After
    public void closeUp() throws SQLException {

        connect.close();

    }

    @Test
    public void insertTableDaoList() throws Exception {
        ArrayList<Mysql_Dao> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Mock_OrderMysqlAbsctractMock mockOrder = new Mock_OrderMysqlAbsctractMock();
            mockOrder.mock();
            list.add(mockOrder);
            System.out.println(mockOrder);
        }
        Mysql_Tool.insertTableDaoList(connect,"mock_order", Mock_OrderMysqlAbsctractMock.class.getName(),list);
    }

    @Test
    public void tableQueryset() throws Exception {
        ArrayList<Mysql_Dao> list = Mysql_Tool.tableQueryset(connect, "mock_order", Mock_OrderMysqlAbsctractMock.class.getName(), "status=0");
        for (Mysql_Dao mysqlDao : list) {
            System.out.println(mysqlDao);
        }
    }


    @Test
    public  void tableToObjectCode() throws Exception {
        Mysql_Tool.tableToObjectCode(connect,"mock_cupon");

    }

}