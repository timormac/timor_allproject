package com.lpc.datamock.excute;

import com.lpc.datamock.functions.A2_UserInfoMock;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Title: UserInfoExcutorRealtime
 * @Package: excute
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 21:05
 * @Version:1.0
 */
public class UserInfoExcutorRealtime {
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        A2_UserInfoMock mock = new A2_UserInfoMock();
        mock.MockAndInsertRealtime();

    }
}
