package com.lpc.datamock.dao;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @Title: Spu
 * @Package: dao
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 16:36
 * @Version:1.0
 */
public class Spu implements Dao{

    private int id;
    private String spu_name;
    private Double price;
    private Integer merchantid;
    private Timestamp create_time;
    private Timestamp modify_time;

    public Spu(String spu_name, Double price, Integer merchantid, Long create_time, Long modify_time) {
        this.spu_name = spu_name;
        this.price = price;
        this.merchantid = merchantid;
        this.create_time =  new Timestamp(create_time)  ;
        this.modify_time = new Timestamp(modify_time) ;
    }

    public Spu() {}


    @Override
    public Dao getInstance(ArrayList<String> arr) {

        this.id=  Integer.valueOf(arr.get(0)) ;
        this.spu_name = arr.get(1);
        this.price = Double.valueOf(arr.get(2));
        this.merchantid = Integer.valueOf(arr.get(3));
//            this.create_time = new Timestamp( Long.valueOf(arr.get(4))  )  ;
//            this.modify_time = new Timestamp( Long.valueOf(arr.get(5)) ) ;

        return this;
    }


    public Spu(int id,double price) {
        this.id = id;
        this.price = price;
    }

    public Spu(String spu_name) {
        this.spu_name = spu_name;
    }

    public int getId() {
        return id;
    }

    public String getSpu_name() {
        return spu_name;
    }

    public Double getPrice() {
        return price;
    }

    public Integer getMerchantid() {
        return merchantid;
    }

    public Timestamp getCreate_time() {
        return create_time;
    }

    public Timestamp getModify_time() {
        return modify_time;
    }

    @Override
    public String toString() {
        return "Spu{" +
                "spu_name='" + spu_name + '\'' +
                ", price=" + price +
                ", merchantid=" + merchantid +
                ", create_time=" + create_time +
                ", modify_time=" + modify_time +
                '}';
    }
}
