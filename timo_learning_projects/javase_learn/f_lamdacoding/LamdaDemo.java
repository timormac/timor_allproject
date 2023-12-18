package f_lamdacoding;

/**
 * @Title: LamdaDemo
 * @Package: f_lamdacoding
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/25 08:58
 * @Version:1.0
 */
public class LamdaDemo {

    public static void main(String[] args) {

        // lamada表达式可以快速实现接口创建对象,前提是接口只有1个实现方法

        //无参数的lamda实现接口
        Inter1  obj1 = () -> { System.out.println("aa");  };
        obj1.method();

        //有参的lamda实现接口
        Inter2  obj3 = (name,age) -> {
            System.out.println("bb");
            return age+1;
        };
        obj3.method("kk",10);

        //并不一定需要{},如果返回值操作不复杂的化直接可以-> age ，不需要括号还有return
        Inter2  obj2 = (name,age) -> age+1 ;
        obj2.method("mm",12);

        //单参数时参数的()也可以省略
        Inter3  obj4 = age -> age+1 ;

/*        //当接口的参数是范型时，在lamda中要表明参数类型
        Inter4  inter4 = (String s,String i) -> {
            System.out.println(s);
        };
        */

        //当有一个类的静态方法和接口的参数和返回类型相同时,可以分享方法
        Inter2   obj5 = C1::share;
        obj5.method("mm",20);

        //若不是静态方法要new对象分享方法
        Inter2  obj6 = new C2()::share;
        obj6.method("tt",50);


    }


}

interface Inter1{
    void method();
}

interface Inter2{
    int method(String name,int age);
}

interface Inter3{
    int  method(int age);
}

interface Inter4<T,D>{
     void method(T t,D d);
}

class C1{
    public static int  share(String name,int age){

        System.out.println("我是静态方法");
        return age;
    }
}

class C2{
    public int  share(String name,int age){

        System.out.println("非态方法");
        return age;
    }
}
