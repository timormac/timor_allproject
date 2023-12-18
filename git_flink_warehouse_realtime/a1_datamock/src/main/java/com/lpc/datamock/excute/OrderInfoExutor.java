package com.lpc.datamock.excute;

import com.lpc.datamock.functions.A3_OrderInofoMock;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Title: OrderInfoExutor
 * @Package: excute
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 22:51
 * @Version:1.0
 */
public class OrderInfoExutor {
    public static void main(String[] args) throws SQLException, IOException {

        A3_OrderInofoMock mock = new A3_OrderInofoMock();
        mock.initialMockAndInsert(1000000);


    }
}
