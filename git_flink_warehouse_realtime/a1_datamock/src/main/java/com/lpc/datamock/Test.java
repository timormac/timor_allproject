package com.lpc.datamock;

/**
 * @Author Timor
 * @Date 2023/12/1 12:05
 * @Version 1.0
 */
public class Test {
    public static void main(String[] args) {

        char  c= (char) (0x4e00 + (int) (Math.random() * (0x9fa5 - 0x4e00 + 1)));
        String s ="";
        for (int i = 0; i < 100; i++) s+=c;



        System.out.println(s);

    }
}
