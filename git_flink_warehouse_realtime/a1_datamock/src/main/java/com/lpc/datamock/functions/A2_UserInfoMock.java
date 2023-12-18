package com.lpc.datamock.functions;

import com.lpc.datamock.dao.UserInfo;
import com.lpc.datamock.tools.A1_JDBCPool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @Title: A2_UserInfoMock
 * @Package: functions
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 20:41
 * @Version:1.0
 */
public class A2_UserInfoMock {

    private Connection connect;

    public A2_UserInfoMock() throws SQLException, IOException {
        getPoolConnect();
    }


    public void MockAndInsert(int user_num) throws SQLException {
        String sql = "insert into userinfo(name,create_time,modify_time) values(?,?,?)";
        PreparedStatement pst = this.connect.prepareStatement(sql);

        for (int i = 0; i <user_num ; i++) {
            UserInfo user = getOneData();
            dataIntoStatement(pst,user);
            pst.addBatch();
        }
        pst.executeBatch();
        pst.close();
        this.connect.close();
    }

   public void MockAndInsertRealtime() throws SQLException, InterruptedException {
        String sql = "insert into userinfo(name,create_time,modify_time) values(?,?,?)";
        PreparedStatement pst = this.connect.prepareStatement(sql);
        while (true){
            UserInfo user = getOneData();
            dataIntoStatement(pst,user);
            pst.execute();
            System.out.println(  "user:" + user.getName()+user.getCreate_time() );
            Thread.sleep(25*1000);
        }

    }


    void dataIntoStatement(PreparedStatement pst,UserInfo user ) throws SQLException {
        pst.setString(1, user.getName());
        pst.setTimestamp(2, user.getCreate_time());
        pst.setTimestamp(3, user.getModify_time());
    }



    String  getName(){
        String[]  aArr = new String[]{"赵","钱","孙","李","周","吴","郑","王"};
        String[]  nameArr = new String[]{"小米","华为","苹果","三星","诺基亚"};
        String[]  codeArr = new String[]{"A","B","C","D","E","F","G","H","J","K","T"};
        String name;
        int floor = (int)Math.floor(Math.random() * 10000);
        int  a = (int)Math.floor(Math.random()*7);
        int  nameNum = (int)Math.floor(Math.random()*4);
        int  codeNum = (int)Math.floor(Math.random()*10);
        name = aArr[a] + nameArr[nameNum]+codeArr[codeNum]+floor;
        return name;
    }

    UserInfo getOneData(){
        String name = getName();
        long l = System.currentTimeMillis();
        Timestamp create = new Timestamp(l);
        return  new UserInfo(name,create,create);

    }

   void getPoolConnect() throws IOException, SQLException {

       this.connect = A1_JDBCPool.getPoolConnect();
   }





}
