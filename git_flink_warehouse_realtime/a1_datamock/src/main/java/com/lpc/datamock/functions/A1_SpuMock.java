package com.lpc.datamock.functions;

import com.lpc.datamock.dao.Spu;
import com.lpc.datamock.tools.A1_JDBCPool;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * @Title: A1_SpuMock
 * @Package: functions
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 16:08
 * @Version:1.0
 */
public class A1_SpuMock {

    private Connection connect;
    private HashSet<String> spuNameSet = new HashSet<>();

    private ArrayList<Spu> spuList = new ArrayList<>();

    public A1_SpuMock() throws IOException, SQLException {
        //每次创建数据前，先加载mysql里面的spunam，确保不重复
        this.connect = A1_JDBCPool.getPoolConnect();
        updateSpuNameSet();

    }

    //从数据库获取数据，更新spuNameSet集合
    private void updateSpuNameSet() throws SQLException {

        String sql ="select spu_name from spu";
        PreparedStatement state = connect.prepareStatement(sql);
        ResultSet resultSet = state.executeQuery();
        //从新重mysql获取
        while (resultSet.next()) {
            spuNameSet.add(resultSet.getString("spu_name"));
        }
        state.close();
        System.out.println("从mysql拉数据写入spuNameSet");
    }

    //产生数据并插入数据库,参数是需要数据条数
    public  void mockAndInsert( int data_num ) throws SQLException {
        String sql = "insert into spu(spu_name,price,merchantid,create_time,modify_time) values(?,?,?,?,?)";
        //这里先传sql,mysl服务器会对这个sql预编译
        PreparedStatement pst = this.connect.prepareStatement(sql);

        for (int i = 0; i <data_num ; i++) {
            Spu spu = reduceOneData();
            dataIntoStatement(pst,spu);
            pst.addBatch();
        }

        pst.executeBatch();
        pst.close();
        this.connect.close();
    }

    //实时产生数据
    public void mockAndInsertRealtime() throws SQLException, InterruptedException {
        String sql = "insert into spu(spu_name,price,merchantid,create_time,modify_time) values(?,?,?,?,?)";
        //这里先传sql,mysl服务器会对这个sql预编译
        PreparedStatement pst = this.connect.prepareStatement(sql);

        while (true){
            Spu spu = reduceOneData();
            dataIntoStatement(pst,spu);
            pst.execute();
            System.out.println("spu:"+getSPuname()+spu.getCreate_time());
            //休眠5分钟
            Thread.sleep(5*60*1000);
        }


    }

    //这个方法目前有问题不用，想要提取出来，不过for循环的时候,循环执行connect.prepareStatement
    private void dataIntoStatement(PreparedStatement pst,Spu spu) throws SQLException {
        pst.setString(1,spu.getSpu_name());
        pst.setDouble(2,spu.getPrice());
        pst.setInt(3,spu.getMerchantid());
        pst.setTimestamp(4,spu.getCreate_time());
        pst.setTimestamp(5,spu.getModify_time());
    }



    private Spu reduceOneData(){

        String name = getSPuname();
        double price = getPrice();
        long create = System.currentTimeMillis();
        Spu spu = new Spu(name, price, 1, create, create);
        return spu;

    }

     private double getPrice(){
        //随机获取0-1小数
        double random = Math.random()*1000;
        //转为strin保留后2位
        String format = String.format("%.2f", random);
        Double d = Double.valueOf(format);
        return  d;

    }

    private String getSPuname(){
        String[]  nameArr = new String[]{"小米","华为","苹果","HTC-","三星","vivo-","诺基亚"};
        String[]  codeArr = new String[]{"A","B","C","D","E","F","G","H","J","K","T"};
        String name;
        int sign=0;
        do{
            int floor = (int)Math.floor(Math.random() * 10000);
            int  nameNum = (int)Math.floor(Math.random()*6);
            int  codeNum = (int)Math.floor(Math.random()*10);
            name = nameArr[nameNum]+codeArr[codeNum]+floor;
            if(spuNameSet.contains(name) && sign!=0 ) {
                System.out.println("名字重复重新新建名称");
            }
            sign+=1;
        } while( spuNameSet.contains(name) );

        spuNameSet.add(name);

        return name;
    }

}
