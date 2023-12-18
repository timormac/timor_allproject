package c1_collections;

import java.util.HashSet;

/**
 * @Title: A3_CollectionSet
 * @Package: c1_collections
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/27 11:21
 * @Version:1.0
 */
public class A3_Set {
    public static void main(String[] args) {

        HashSet<String> set = new HashSet<>();
        set.add("a");
        System.out.println(set.contains("a"));
        System.out.println(set.contains("b"));



    }
}
