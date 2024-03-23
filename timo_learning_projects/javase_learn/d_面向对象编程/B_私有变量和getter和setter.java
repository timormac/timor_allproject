package d_面向对象编程;

/**
 * Author: timor
 * Date:2023/3/13 20:08
 */
public class B_私有变量和getter和setter {

    private  int legs ;

    public void setlegs ( int num ){

        if( num > 0 && num %2 == 0 ){

            this.legs = num ;

        }

    }

    public int getlegs( ) {

        return this.legs ;

    }

    public static void main(String[] args) {

        B_私有变量和getter和setter obj =  new B_私有变量和getter和setter() ;

        obj.setlegs(4);

        System.out.println( obj.getlegs());

    }


}
