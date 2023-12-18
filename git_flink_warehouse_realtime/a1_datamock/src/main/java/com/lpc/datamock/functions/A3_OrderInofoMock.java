package com.lpc.datamock.functions;

import com.lpc.datamock.dao.OrderInfo;
import com.lpc.datamock.dao.Spu;
import com.lpc.datamock.dao.UserInfo;
import com.lpc.datamock.tools.A1_JDBCPool;
import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @Title: A3_OrderInofoMock
 * @Package: functions
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 21:26
 * @Version:1.0
 */
public class A3_OrderInofoMock {

    private HashSet<String> orderNoSet = new HashSet<>();
    Connection connect;
    ArrayList<UserInfo>  userList = new ArrayList<>();
    ArrayList<Spu>  spuList = new ArrayList<>();

    public A3_OrderInofoMock() throws IOException, SQLException {
        this.connect =A1_JDBCPool.getPoolConnect();
        refreshSpuList();
        refreshUserList();
    }

   void refreshUserList() throws SQLException {
        String sql = "select id from userinfo";
       PreparedStatement pst = connect.prepareStatement(sql);
       ResultSet resultSet = pst.executeQuery(sql);
       while (resultSet.next()){
           UserInfo user = new UserInfo(resultSet.getInt("id"));
           userList.add(  user );
       }
       pst.close();
   }

    void refreshSpuList() throws SQLException {
        String sql = "select id,price from spu";
        PreparedStatement pst = connect.prepareStatement(sql);
        ResultSet resultSet = pst.executeQuery(sql);
        while (resultSet.next()){
            Spu spu = new Spu( resultSet.getInt("id"),  resultSet.getDouble("price") );
            spuList.add(  spu );
        }
        pst.close();
    }

    public void initialMockAndInsert(int rows) throws SQLException {

        String sql = "insert into orderinfo( orderno,userid,spuid,skuid,amounts,price,cuponno,paid,"
                + "area,create_time,modify_time,remark) values(?,?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pst = connect.prepareStatement(sql);

        for (int i = 0; i < rows; i++) {
            OrderInfo order = initialGetOneData();
            pst.setString(1,order.getOrderno() );
            pst.setInt(2,order.getUserid() );
            pst.setInt(3,order.getSpuid() );
            pst.setInt(4,order.getSkuid() );
            pst.setDouble(5,order.getAmounts() );
            pst.setDouble(6,order.getPrice());
            pst.setString(7,order.getCuponno());
            pst.setDouble(8,order.getPaid());
            pst.setString(9,order.getArea());
            pst.setTimestamp(10,order.getCreate());
            pst.setTimestamp(11,order.getModify());
            pst.setString(12,order.getRemark());
            pst.addBatch();

            if(i%1000==0) {
                pst.executeBatch();
                pst.clearBatch();
                System.out.println(i);
            }
        }

        pst.close();
        connect.close();
    }

    public void mockAndInsertRealtime() throws SQLException, InterruptedException {

        String sql = "insert into orderinfo( orderno,userid,spuid,skuid,amounts,price,cuponno,paid,"
                + "area,create_time,modify_time) values(?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pst = connect.prepareStatement(sql);

        while (true){
            OrderInfo order = getOneData();
            pst.setString(1,order.getOrderno() );
            pst.setInt(2,order.getUserid() );
            pst.setInt(3,order.getSpuid() );
            pst.setInt(4,order.getSkuid() );
            pst.setDouble(5,order.getAmounts() );
            pst.setDouble(6,order.getPrice());
            pst.setString(7,order.getCuponno());
            pst.setDouble(8,order.getPaid());
            pst.setString(9,order.getArea());
            pst.setTimestamp(10,order.getCreate());
            pst.setTimestamp(11,order.getModify());
            pst.execute();
            int sleep = (int)Math.floor(Math.random()*10) ;
            System.out.println( "order:" + order.getOrderno() +order.getCreate() );
            Thread.sleep(1000*sleep);
        }




    }


    OrderInfo
    getOneData(){
        String orderno = getOrderno();
        int userid = getUserid();
        Spu spu = getSpu();
        int spuid = spu.getId();
        int skuid = 0;
        int amounts = (int)( Math.floor(Math.random()*4 ) +1 );
        double price = spu.getPrice();
        String cuponno ="0";
        double paid = price*amounts;
        String arae = "0";
        long l = System.currentTimeMillis();
        Timestamp  create =  new Timestamp(l);
        String remark = getRemark();
        OrderInfo order = new OrderInfo(orderno,userid,spuid,skuid,amounts,price,cuponno,paid,arae,create,create,remark);
        return  order;

    }


    OrderInfo initialGetOneData(){

        String orderno = initialGetOrderno();
        int userid = getUserid();
        Spu spu = getSpu();
        int spuid = spu.getId();
        int skuid = 0;
        int amounts = (int)( Math.floor(Math.random()*4 ) +1 );
        double price = spu.getPrice();
        String cuponno ="0";
        double paid = price*amounts;
        String arae = "0";
        long l = System.currentTimeMillis() - (int) Math.floor(Math.random()*1000*3600*24*100 );
        Timestamp  create =  new Timestamp(l);
        String remark = getRemark();
        OrderInfo order = new OrderInfo(orderno,userid,spuid,skuid,amounts,price,cuponno,paid,arae,create,create,remark);

        return  order;

    }

    //目前实现不了根据类的数据类型动态 getString,或者getInt
//    <V> void refreshList(String sql, ArrayList<V> list,String[] colArr) throws SQLException {
//
//        PreparedStatement pst = connect.prepareStatement(sql);
//        ResultSet resultSet = pst.executeQuery(sql);
//
//        while (resultSet.next()){
//            Spu spu = new Spu( resultSet.getInt("id"),  resultSet.getDouble("price") );
//            spuList.add(  spu );
//        }
//        pst.close();
//    }


   String getOrderno(){
       String orderno ="D";
       long stamp = System.currentTimeMillis();
       String str = String.valueOf(stamp);
       orderno = orderno +str;
       return orderno;
   }

   String initialGetOrderno(){

        String orderNo ="D";
        int flag= 0;
        do{
            if(flag>1) System.out.println("set订单号重复,重新随机订单号");
            long stamp = System.currentTimeMillis();
            String substring = String.valueOf(stamp).substring(5);
            int f1 =(int)Math.floor(Math.random() * 10000);
            int f2 =(int)Math.floor(Math.random() * 10000);
            orderNo = orderNo +substring+f1+f2;
            flag++;
        }while (orderNoSet.contains(orderNo));
       orderNoSet.add(orderNo);
       return orderNo;

   }

  int getUserid(){
      int size = userList.size();
      int index = (int) Math.floor(Math.random() * size);
      return userList.get(index).getId();
  }

    Spu getSpu(){
        int size = spuList.size();
        int index = (int) Math.floor(Math.random() * size);

        return spuList.get(index);
    }

    String getRemark(){
        char  c= (char) (0x4e00 + (int) (Math.random() * (0x9fa5 - 0x4e00 + 1)));
        String s ="";
        for (int i = 0; i < 100; i++) s+=c;
        return s;
    }


}
