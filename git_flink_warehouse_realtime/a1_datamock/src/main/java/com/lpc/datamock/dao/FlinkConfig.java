package com.lpc.datamock.dao;

import java.util.ArrayList;

/**
 * @Author Timor
 * @Date 2023/10/27 17:59
 * @Version 1.0
 */
public class FlinkConfig implements  Dao {

    Integer id;
    String  table_name;


    public FlinkConfig() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    @Override
    public Dao getInstance(ArrayList<String> arr) {
        this.id=  Integer.valueOf(arr.get(0)) ;
        this.table_name = arr.get(1);
        return this;
    }
}
