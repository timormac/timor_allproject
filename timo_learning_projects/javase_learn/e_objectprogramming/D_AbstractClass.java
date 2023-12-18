package e_objectprogramming;


/**
 * Author: timor
 * Date:2023/3/13 22:48
 */



//抽象类不能创建对象，子类只有实现了所有抽象方法才能创建对象
//抽象类可以实现接口
public abstract class D_AbstractClass {

     public  abstract void speak() ;

     void instancMethod (){
         System.out.println("有实例方法");
     }
}

interface inter1{
    void inter1M1();
    void inter1M2();
}
interface inter2{
    void inter2m();
}

abstract   class abstractDemo implements inter1,inter2{
    @Override
    public void inter1M1() {
        System.out.println(1);
    }

    @Override
    public void inter1M2() {
        System.out.println(2);
    }


}

class ExtendsDemo extends abstractDemo{
    @Override
    public void inter2m() {
        System.out.println(3);
    }
}

class Excutor{
    public static void main(String[] args) {
        ExtendsDemo extendsDemo = new ExtendsDemo();
        extendsDemo.inter1M1();
    }
}
