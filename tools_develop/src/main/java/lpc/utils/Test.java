package lpc.utils;

import lpc.utils.udfclass.UdfTuple2;

import java.util.HashSet;

/**
 * @Title: Test
 * @Package: lpc.utils
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/21 03:27
 * @Version:1.0
 */
public class Test {
    public static void main(String[] args) {

        HashSet<Integer> set = new HashSet<>();
        set.add(1);
        set.add(2);

        UdfTuple2<String, HashSet<Integer>> tuple = new UdfTuple2<String, HashSet<Integer>>("a",set);

        String excute_sql = tuple.f1();
        HashSet<Integer> integers = tuple.f2();
        System.out.println(excute_sql);
        System.out.println(integers);
        System.out.println(tuple);
    }
}
