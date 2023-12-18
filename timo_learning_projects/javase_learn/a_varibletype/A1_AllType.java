package a_varibletype;

public class A1_AllType {

    public static void main(String[] args) {

        // 基本类型
        byte  bt= 1 ;
        short st= 2 ;
        int   var1 =10 ;
        long  l1 =1234L ;

        float  f1 = 1.24F ;
        double d1 = 1.24D ;

        //char 可以储存特殊字符 如 /t /n等
        char ch1 = 'a' ;
        char ch2 = '\n' ;

        boolean  var3 = false ;



        //引用类型
        String   str = "aaa";

        //数组
        int[]  arr = new int[]{  1,2,3,4} ;
        int[]  arr2 = new int[4] ;

        int[][]  arr_2 = new int[3][4] ;

        System.out.println( arr2.length );

        for( int i =0 ; i < arr2.length ; i++  ){

            arr2[i] = i*2 ;

        }

       // System.out.println( arr2 );




        //这里也是一个写法，声明一个数组,不过奇怪
        Object[]  objArr = {} ;




    }



}
