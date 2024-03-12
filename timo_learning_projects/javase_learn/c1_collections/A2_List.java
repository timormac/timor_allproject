package c1_collections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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

       //排序,传一个Comparator,注意compareTO返回的是-1,0,1
       list.sort(new Comparator<String>() {
           @Override
           public int compare(String o1, String o2) {
               return o1.compareTo(o2);
           }
       });



       //TODO Arrays,将int[]转为List,再新建一个new Arrlist( list )，就可以了
        Integer[]  arr = new Integer[]{1,2,3};
        List<Integer> integers = Arrays.asList(arr);
        ArrayList<Integer> arrList = new ArrayList<>(integers);


        //还有Vector,Stack,LinkedList都是List接口的实现类
        // Vector相比ArrayList 线程安全，效率低
        //stack比List多出pop,push弹栈，压栈方法
        //LinkedList是链表结构，方便元属添加，删除

    }
}
