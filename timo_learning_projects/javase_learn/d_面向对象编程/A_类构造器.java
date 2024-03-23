package d_面向对象编程;


public class A_类构造器 {


    //默认值是0
    int age ;

    String name ;

    String type ;

    //默认值是null
    int[] array;


    //不设置构造器默认提供一个无参构造器，设置过构造器默认的无参构造器不提供
    private A_类构造器(){

    }

    public A_类构造器(int age , String name ){

        //调用无参构造器应该是可用可不用写的。如果要是写必须写第一行
        this() ;
        this.age = age ;
        this.name = name;

    }

    public A_类构造器(int age , String name , int[] arr ){

        this(age,name ) ;
        this.array = arr ;

    }


    public static void main(String[] args) {

        //虽然应私有化无参构造器了，但是还是能调用无参构造器
        A_类构造器 obj =  new A_类构造器() ;

        A_类构造器 obj2 =  new A_类构造器(1 ,"aa") ;



        System.out.println(obj.name);



    }

}
