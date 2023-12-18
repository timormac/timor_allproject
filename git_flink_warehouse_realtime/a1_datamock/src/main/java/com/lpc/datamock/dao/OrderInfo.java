package com.lpc.datamock.dao;

import java.sql.Timestamp;

/**
 * @Title: OrderInfo
 * @Package: dao
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 21:11
 * @Version:1.0
 */
public class OrderInfo  {

    private int id;
    private String orderno;
    private int userid;
    private int spuid;
    private int skuid;
    private int amounts;
    private double  price;
    private String cuponno;
    private double paid;
    private String  area;
    private Timestamp create;
    private Timestamp modify;
    private String remark;


    public OrderInfo(String orderno, int userid, int spuid, int skuid, int amounts,
                     double price, String cuponno, double paid, String area, Timestamp create, Timestamp modify, String remark) {
        this.orderno = orderno;
        this.userid = userid;
        this.spuid = spuid;
        this.skuid = skuid;
        this.amounts = amounts;
        this.price = price;
        this.cuponno = cuponno;
        this.paid = paid;
        this.area = area;
        this.create = create;
        this.modify = modify;
        this.remark = remark;
    }

    public String getOrderno() {
        return orderno;
    }

    public int getUserid() {
        return userid;
    }

    public int getSpuid() {
        return spuid;
    }

    public int getSkuid() {
        return skuid;
    }

    public int getAmounts() {
        return amounts;
    }

    public double getPrice() {
        return price;
    }

    public String getCuponno() {
        return cuponno;
    }

    public double getPaid() {
        return paid;
    }

    public String getArea() {
        return area;
    }

    public Timestamp getCreate() {
        return create;
    }

    public Timestamp getModify() {
        return modify;
    }
    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }


}
