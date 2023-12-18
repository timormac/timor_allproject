package e_objectprogramming;


/**
 * Author: timor
 * Date:2023/3/13 22:11
 */
public class C_OverwriteEqualsMethod  {

    String name ;
    int   age ;

    public static void main(String[] args) {

        C_OverwriteEqualsMethod c1 = new C_OverwriteEqualsMethod("aa",18) ;

        C_OverwriteEqualsMethod c2 = new C_OverwriteEqualsMethod( "aa",18);

        C_OverwriteEqualsMethod c3 = new C_OverwriteEqualsMethod( "aa",17);

        System.out.println( c1 == c2 );
        System.out.println( c1.equals( c2 ));
        System.out.println( c1.equals(c3));

        String a1 ="aa" ;
        String a2 = "aa" ;

        System.out.println( a1==a2);



    }

    public C_OverwriteEqualsMethod(String name ,int age  ){

        this.name = name ;
        this.age = age ;

    }




    public boolean equals(Object obj) {

        if( this == obj ){
            return true ;
        }

        if(obj instanceof C_OverwriteEqualsMethod){

            C_OverwriteEqualsMethod p1 = (C_OverwriteEqualsMethod) obj ;

            if( p1.name.equals( this.name) && p1.age == this.age ){

                return true ;
            }

            return false ;

        }

        return false ;

    }

}
