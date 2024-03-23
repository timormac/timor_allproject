package d_面向对象编程;

/**
 * Author: timor
 * Date:2023/3/13 23:15
 */
public class F_Annotation extends C_方法的重载和重写 {

    //因为父类没有无参构造器，所以继承的时候，必须声明一个构造器，很奇怪，多此一举
    public F_Annotation(String name ,int age ){
        super(name,age);

    }

    @Override  //声明这个这个方法已经重写过了
    public boolean equals(Object obj) {

        if( this == obj ){
            return true ;
        }

        if(obj instanceof C_方法的重载和重写){

            C_方法的重载和重写 p1 = (C_方法的重载和重写) obj ;

            if( p1.name.equals( this.name) && p1.age == this.age ){

                return true ;
            }

            return false ;

        }

        return false ;

    }

}
