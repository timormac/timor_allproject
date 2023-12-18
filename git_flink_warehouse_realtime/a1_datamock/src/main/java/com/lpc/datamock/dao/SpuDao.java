package com.lpc.datamock.dao;

import java.util.ArrayList;

/**
 * @Author Timor
 * @Date 2023/11/2 19:28
 * @Version 1.0
 */
public class SpuDao implements Dao {

    Integer id;
    String spu_name;
    Double price;
    Integer merchantid;
    String create_time;
    String modify_time;

    public SpuDao() {
    }

    public SpuDao(Integer id, String spu_name, Double price, Integer merchantid, String create_time, String modify_time) {
        this.id = id;
        this.spu_name = spu_name;
        this.price = price;
        this.merchantid = merchantid;
        this.create_time = create_time;
        this.modify_time = modify_time;
    }


    @Override
    public Dao getInstance(ArrayList<String> arr) {
        this.id=  Integer.valueOf(arr.get(0)) ;
        this.spu_name = arr.get(1);
        this.price = Double.valueOf(arr.get(2));
        this.merchantid = Integer.valueOf(arr.get(3));
        this.create_time = arr.get(4);
        this.modify_time = arr.get(5);
        return  this;
    }


    @Override
    public String toString() {
        return "SpuDao{" +
                "id=" + id +
                ", spu_name='" + spu_name + '\'' +
                ", price=" + price +
                ", merchantid=" + merchantid +
                ", create_time='" + create_time + '\'' +
                ", modify_time='" + modify_time + '\'' +
                '}';
    }




}
