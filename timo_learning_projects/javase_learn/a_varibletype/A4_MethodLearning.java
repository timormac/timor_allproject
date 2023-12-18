package a_varibletype;

public class A4_MethodLearning {

    //可变参数，如果有多个参数可变参数必须放在最后一个参数,格式为类型...，后面会自动封装进一个数组里面
    public static void methodActiveParameter( int...  args ){}

    //可变参数方法2
    public static void ActiveParameter( int[]  args ){}

    //方法重载 同一个方法，参数列表不同，会执行不同的方法
    public static   int methodOverlode( int  i  ){return 1 ;}
    public  static  String methodOverload( String str ) { return str; } ;






}
