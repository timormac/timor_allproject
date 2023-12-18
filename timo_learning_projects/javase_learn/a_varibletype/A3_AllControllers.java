package a_varibletype;

public class A3_AllControllers {

    public static void main(String[] args) {
        
        double  num = Math.random()*10;
        int  n2=0;


        //三元表达式 num是否大于10。5s如果满足 n2为10否则100
         n2 = num >10.5 ? 10:100;

        
        //if第一种写法
        if( num >1 ) {
            System.out.println("1");
        }

        //简单缩写写法,当{}里面逻辑简单可以省略
        if(num <1)  num=10 ;
        
        //if第二种写法
        if( num >1 ){
            System.out.println(1);
        }else{
            System.out.println(2);
        }

        //省略写法
        if( num >1 ) System.out.println(1);
        else  System.out.println(2);


        //if第三种写法
        if (num>5){
            System.out.println("1");
        } else if (num>3) {
            System.out.println(1);
        }

        //省略写法
        if (num>5) System.out.println("1");
        else if (num>3)  System.out.println(1);







        //3元运算符
         int  n1   = num > 5 ? 1 : 2;

        //switch和if区别,switch只能做常量相等判断，if可以做条件判断，default为case都未匹配到执行的
        //switch第一种写法带break
        switch((int) num){
            case 2:
                System.out.println(2);
                break;
            case 3:
                System.out.println(3);
                break;
            default:
                System.out.println("无");
                break;
        }

        //switch第二种写法，如果没有break会将符合case的都执行一次直到遇到brea；
        switch( (int)num ) {
            case 1:
            case 2:
            case 12:
                System.out.println("冬季");
                break;
            case 3:
            case 4:
            case 5:
                System.out.println("春季");
                break;
            default:
                System.out.println("你输入的月份有误");
                break;
        }


        //for循环第一种
        //循环结构 for循环  格式 for( 声明变量 ; 变量条件;，执行{}操作后在执行这里  ){条件满足先执行这里}
        for( int j = 0  ; j< 5 ; j++ ){
            System.out.println( "执行一次目前j是"+ j );
        }

        int[]  array = new int[]{1,2,3};

        //for循环第二种
        for (int i : array) {
            System.out.println(i);
        }

        //for 循环简单写法,省略大括号
        for (int i : array) System.out.println(i);


        //while循环第一种写法
        int w=1 ;
        while (w <5){
            System.out.println( w );
            w +=1 ;
        }

        //while循环第二种写法,用break跳出
        while(true){
            w+=1;
            if(w>5){
                break;
            }
        }



        //do while循环 先执行循环体，必定执行1次,循环体写在do里面
        do{
             w+=1;
        } while( w<10);

        //break 用于结束整体循环，continue用于结束本次循环





    }
}
