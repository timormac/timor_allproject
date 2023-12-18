package com.lpc.datamock.excute;

import com.lpc.datamock.functions.A1_SpuMock;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Title: SpuExcutorRealtime
 * @Package: excute
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 17:58
 * @Version:1.0
 */
public class SpuExcutorRealtime {
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {

        A1_SpuMock mock = new A1_SpuMock();
        mock.mockAndInsertRealtime();

    }
}
