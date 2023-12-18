package g_generictype;

import java.util.ArrayList;
import java.util.List;

/**
 * @Title: ClassInterMethodGenericType
 * @Package: g_generictype
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/15 16:49
 * @Version:1.0
 */

//范型能用在类，接口，方法里, 并且范型可以指定继承了哪些类X extends Man
public class ClassMethodGenericType<T,X extends Man > {

    T  t;

    void add(T t){}

    T get(){
        return null;
    }



    //方法范型，这里范型适用范围只能在本方法里
     public <W> void sort(W w){}

    //当返回值是一个带范型的对象，那么方法范型也要写在返回值之前
    <M> Man<M> getMan(){ return new Man(); }

    //虽然Man是Person的子类，但是配合List就无法形成多态,func1传入List<Man>时，无法通过编译
    static void  func1(  List<Person> list ){}

    //解决方式,这种可以接Person子类对应的List
    static void  func2(  List<? extends  Person> list ){}


    //测试
    public static void main(String[] args) {

        ClassMethodGenericType<Object,Man> obj = new ClassMethodGenericType<>();

        //范型方法指定范型在方法前面写
        Man<String> man = obj.<String>getMan();
        List<Man> arr1 = new ArrayList<>();

       //会报错，接不住
       //func1(arr1);


    }

}
