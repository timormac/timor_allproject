package com.lpc.datamock.excute;

import com.lpc.datamock.functions.A2_UserInfoMock;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Title: UserInfoExcutor
 * @Package: excute
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 21:02
 * @Version:1.0
 */
public class UserInfoExcutor {
    public static void main(String[] args) throws SQLException, IOException {
        A2_UserInfoMock mock = new A2_UserInfoMock();
        mock.MockAndInsert(2000);
    }
}
