package lpc.utils.mysql.dao;

import java.util.ArrayList;
import java.util.Map;

/**
 * @Author Timor
 * @Date 2024/2/24 16:35
 * @Version 1.0
 */

//这是个mysql表对应的数据类
public interface Dao {

    //在A2_TableQueryDao中会查询数据集然后把查询结果，按字段顺序放入arr中
    Dao getInstance(Map<String,Object> map) throws IllegalAccessException;
}
