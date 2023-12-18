package 装饰器设计模式;

/**
 * @Title: A4_DecoratorExtends
 * @Package: 装饰器设计模式
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 13:00
 * @Version:1.0
 */
public class A4_DecoratorExtends_A extends A3_DecoratorInterface {

    public A4_DecoratorExtends_A(A1_FunctionInterface f1) {
        super(f1);
    }

    @Override
    public void function() {
        super.function();
        System.out.println("这是新加装饰器功能A");
    }
}
