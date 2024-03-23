package b_权限_类_方法是否可跨包_跨类;

/**
 * @Title: A1_非public不可跨包
 * @Package: b_权限_类_方法是否可跨包_跨类
 * @Description:
 * @Author: lpc
 * @Date: 2024/3/23 09:39
 * @Version:1.0
 */
public class A1_非public不可跨包 {

    //TODO 内部public类,同包/跨包引用都要设置为static
    public static   class InnerClass{ }
}


//TODO 非public类无法在别的包引用,public类只能在自己的java文件声明,或者说内部类声明,不能在主类同级声明
//TODO 同级类只能clas，不可以publi,static
class UnPublicClass{}