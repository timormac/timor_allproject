package com.lpc.datamock.dao;

import java.sql.Timestamp;

/**
 * @Title: UserInfo
 * @Package: dao
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 20:38
 * @Version:1.0
 */
public class UserInfo {


    private int id;
    private String name;

    private Timestamp create_time;

    private Timestamp modify_time;

    public UserInfo(String name, Timestamp create_time, Timestamp modify_time) {
        this.name = name;
        this.create_time = create_time;
        this.modify_time = modify_time;
    }



    public UserInfo(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Timestamp getCreate_time() {
        return create_time;
    }

    public Timestamp getModify_time() {
        return modify_time;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
