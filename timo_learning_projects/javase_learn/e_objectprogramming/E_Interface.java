package e_objectprogramming;

/**
 * Author: timor
 * Date:2023/3/13 22:52
 */

//接口只能有抽象方法,和抽象类的区别是:抽象类可以有实例方法
//如果实现类没有实现接口中的全部方法，只能是抽象类
//接口可以多实现，实现了不能多继承的问题。
public interface E_Interface {
    void  speak();
}

interface Interface2{
    void learn( );
}

class DemoImpl implements E_Interface,Interface2{
    @Override
    public void speak() {
        System.out.println("speak");
    }

    @Override
    public void learn() {
        System.out.println("learn");
    }
}