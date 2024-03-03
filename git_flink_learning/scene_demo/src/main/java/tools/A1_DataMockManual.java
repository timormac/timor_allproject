package tools;

import lpc.utils.mysql.dao.Dao;
import lpc.utils.mysql.dao.Mock_Cupon;
import lpc.utils.mysql.dao.Mock_Order;
import lpc.utils.mysql.tools.A1_MysqlTool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @Author Timor
 * @Date 2024/2/24 16:08
 * @Version 1.0
 */
public class A1_DataMockManual {
    public static void main(String[] args) throws Exception {

        Connection connect = A1_MysqlTool.getPoolConnect();

        while (true){


            ArrayList<Dao> order_list = new ArrayList<Dao>();
            Mock_Order mock_order = new Mock_Order();
            mock_order.mock();
            System.out.println(mock_order.userid+ " -|- " + mock_order.orderno);


            order_list.add(mock_order);
            A1_MysqlTool.insertTableDaoList(connect, "mock_order", Mock_Order.class.getName(), order_list);
            order_list.clear();
            Thread.sleep(100);

            ArrayList<Dao> cupon_list = new ArrayList<Dao>();
            Mock_Cupon mock_cupon = new Mock_Cupon();
            mock_cupon.mock(mock_order.orderno);
            System.out.println(mock_cupon);
            cupon_list.add(mock_cupon);
            A1_MysqlTool.insertTableDaoList(connect, "mock_cupon", Mock_Cupon.class.getName(), cupon_list);
            cupon_list.clear();
            Thread.sleep(200);

        }
    }
}
