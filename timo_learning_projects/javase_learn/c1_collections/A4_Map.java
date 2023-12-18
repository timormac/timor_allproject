package c1_collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Title: A4_Map
 * @Package: c1_collections
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/30 11:02
 * @Version:1.0
 */
public class A4_Map {

    public static void main(String[] args) {

        HashMap<String,String> map = new HashMap<>();

        //插入数据
        map.put("a","asd");
        map.put("b","asd");

        //查询操作
        String a = map.get("a");
        boolean b1 = map.containsKey("a");
        boolean b2 = map.containsValue("asd");

        //获取key对应的set
        Set<String> strings = map.keySet();
        //获取对应的values
        Collection<String> values = map.values();

        //获取key映射关系用于遍历
        Set<Map.Entry<String, String>> entries = map.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            System.out.printf(  entry.getKey()+entry.getValue()  );
        }


        //移除指定key
        map.remove("a");


        //清空map
        map.clear();





    }

}
