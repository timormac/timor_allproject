package a4_模拟数据生成;

import lpc.utils.mysql.dao.Dao;
import lpc.utils.mysql.dao.Mock_Cupon;
import lpc.utils.mysql.dao.Mock_Order;
import lpc.utils.mysql.tools.A1_MysqlTool;

import java.sql.Connection;
import java.util.ArrayList;

/**
 * @Title: A1_DataMockManual
 * @Package: tools
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/12 22:31
 * @Version:1.0
 */
public class A1_Mysql数据生成 {
    public static void main(String[] args) throws Exception {

        Connection connect = A1_MysqlTool.getPoolConnect();

        while (true){


            ArrayList<Dao> order_list = new ArrayList<Dao>();
            Mock_Order mock_order = new Mock_Order();
            mock_order.mock();
            System.out.println( mock_order.orderno+ " -|- " +mock_order.spuid);


            order_list.add(mock_order);
            A1_MysqlTool.insertTableDaoList(connect, "mock_order", Mock_Order.class.getName(), order_list);
            order_list.clear();
            Thread.sleep(1000);

            ArrayList<Dao> cupon_list = new ArrayList<Dao>();
            Mock_Cupon mock_cupon = new Mock_Cupon();
            mock_cupon.mock(mock_order.orderno);
            cupon_list.add(mock_cupon);

            System.out.println(mock_cupon.orderno+"-|-"   +mock_cupon.activity);

            A1_MysqlTool.insertTableDaoList(connect, "mock_cupon", Mock_Cupon.class.getName(), cupon_list);
            cupon_list.clear();
            Thread.sleep(1000);

        }
    }
}
