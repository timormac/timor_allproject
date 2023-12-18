package com.lpc.datamock.excute;

import com.lpc.datamock.functions.A3_OrderInofoMock;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Title: OrderInfoExcutorRealtime
 * @Package: excute
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 22:59
 * @Version:1.0
 */
public class OrderInfoExcutorRealtime{
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {

        A3_OrderInofoMock mock = new A3_OrderInofoMock();
        mock.mockAndInsertRealtime();

    }

}
