package f_lamdacoding;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Title: LamadaAsArgument
 * @Package: f_lamdacoding
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/29 14:52
 * @Version:1.0
 */


public class LamadaAsArgument {
    public static void main(String[] args) {
        //lamda表达式作为实现接口传入函数
        FunctionClass<Integer, String> demo = new FunctionClass<>();
        demo.list.add(1);

        //lamda表达式作为实现接口传入函数,lamda表达式不需要处理范型，自动类型推断
        //注意func方法定义时参数TimorMap接口必须带范型，不然会报错
        ArrayList<String> func = demo.func((Integer i)-> "a" + i);

        for (String s : func) {
            System.out.println(s);
        }
    }
}
interface TimorMap<T,V>{
   //范型方法
   V   map(T t);
}
class FunctionClass<T,V>{
    ArrayList<T> list= new ArrayList<>();
    ArrayList<V> result= new ArrayList<>();
    public FunctionClass() {}
    //注意这里的参数必须带范型，不然上面代码会报错
    ArrayList<V> func( TimorMap<T,V> inter ) {
         for (T t : list){  result.add( inter.map(t) ); }

        return result;
    }

}