package com.lpc.datamock.excute;

import com.lpc.datamock.functions.A1_SpuMock;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Title: SpuExcutor
 * @Package: excute
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 17:13
 * @Version:1.0
 */
public class SpuExcutor {

    public static void main(String[] args) throws SQLException, IOException {

        A1_SpuMock mock = new A1_SpuMock();

        mock.mockAndInsert(100);

    }

}
