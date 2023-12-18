package 装饰器设计模式;

/**
 * @Title: Demo
 * @Package: 装饰器设计模式
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 13:04
 * @Version:1.0
 */
public class Demo {
    public static void main(String[] args) {

        A2_Function_1 f1 = new A2_Function_1();
        //A4_DecoratorExtends_A 参数是A1接口,f1实现了A1接口
        A4_DecoratorExtends_A  d_A = new A4_DecoratorExtends_A(f1);
        //A5_DecoratorExtends_A 参数也是是A1接口,d_A继承了A3, A3是实现了A1接口的抽象类
        A5_DecoratorExtends_B d_B = new A5_DecoratorExtends_B(d_A);

        d_B.function();


    }
}
