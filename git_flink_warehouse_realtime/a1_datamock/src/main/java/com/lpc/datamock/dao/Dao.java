package com.lpc.datamock.dao;

import java.util.ArrayList;

/**
 * @Title: Dao
 * @Package: com.lpc.datamock.dao
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/23 21:43
 * @Version:1.0
 */
public interface Dao {

     //在A2_TableQueryDao中会查询数据集然后把查询结果，按字段顺序放入arr中
     Dao getInstance(ArrayList<String> arr);
}
