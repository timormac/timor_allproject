package tools;

import lpc.utils.mysql.dao.Dao;
import lpc.utils.mysql.dao.Mock_Order;
import lpc.utils.mysql.tools.A1_MysqlTool;
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
public class A1_MysqlToolTest {

    private Connection connect;


    @Before
    public void setUp() throws SQLException {
        connect = A1_MysqlTool.getPoolConnect();
    }

    @After
    public void closeUp() throws SQLException {

        connect.close();

    }

    @Test
    public void insertTableDaoList() throws Exception {
        ArrayList<Dao> list = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Mock_Order mockOrder = new Mock_Order();
            mockOrder.mock();
            list.add(mockOrder);
            System.out.println(mockOrder);
        }
        A1_MysqlTool.insertTableDaoList(connect,"mock_order", Mock_Order.class.getName(),list);
    }

    @Test
    public void tableQueryset() throws Exception {
        ArrayList<Dao> list = A1_MysqlTool.tableQueryset(connect, "mock_order", Mock_Order.class.getName(), "status=0");
        for (Dao dao : list) {
            System.out.println(dao);
        }
    }


    @Test
    public  void tableToObjectCode() throws Exception {
        A1_MysqlTool.tableToObjectCode(connect,"mock_cupon");

    }

}