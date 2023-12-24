package com.lpc.realtime.warehouse.a3_dao;

import java.io.Serializable;

/**
 * @Author Timor
 * @Date 2023/11/2 16:58
 * @Version 1.0
 */
public class PojoString implements Serializable {

    public String value;

    public PojoString(String value) {
        this.value = value;
    }

    public PojoString() {
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "PojoString{" +
                "value='" + value + '\'' +
                '}';
    }
}
