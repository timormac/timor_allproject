package e_objectprogramming;

/**
 * Author: timor
 * Date:2023/3/13 20:08
 */
public class B_PrivateAndSetter {

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

        B_PrivateAndSetter obj =  new B_PrivateAndSetter() ;

        obj.setlegs(4);

        System.out.println( obj.getlegs());

    }


}
