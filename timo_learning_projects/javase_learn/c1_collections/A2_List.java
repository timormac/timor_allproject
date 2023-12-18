package c1_collections;

import java.util.ArrayList;
import java.util.List;

/**
 * @Title: A2_CollectionList
 * @Package: c1_collections
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/27 11:07
 * @Version:1.0
 */
public class A2_List {

    public static void main(String[] args) {


       List<String>  list = new  ArrayList<String>();
       //添加元素
       list.add("aa");
       //指定位置添加元素
       list.add(2,"bb");
        //按下标获取元素
        list.get(1);
       //删除指定位置元属，返回被删除元属
       list.remove(2);
       //修改下标对应元素
       list.set(1,"bb");



       //还有Vector,Stack,LinkedList都是List接口的实现类
        // Vector相比ArrayList 线程安全，效率低
        //stack比List多出pop,push弹栈，压栈方法
        //LinkedList是链表结构，方便元属添加，删除

    }
}
