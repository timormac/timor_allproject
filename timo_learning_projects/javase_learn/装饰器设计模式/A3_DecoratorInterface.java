package 装饰器设计模式;

/**
 * @Title: A3_DecoratorInter
 * @Package: 装饰器设计模式
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/20 12:55
 * @Version:1.0
 */


abstract class A3_DecoratorInterface implements A1_FunctionInterface  {

    A1_FunctionInterface  f1;

    public A3_DecoratorInterface(A1_FunctionInterface f1) {
        this.f1 = f1;
    }

    @Override
    public void function() {
        this.f1.function();
    }


}
