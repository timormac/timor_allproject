/**
 * @Title: Test
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/4 18:28
 * @Version:1.0
 */
public class LamdaRealize {

    //手动实现柯里化卡住了，内部类外部接不到
    //想弄个接口,不过只能定义Object为返回值或者参数，
    //但是实现的时候,以为Object能接住Intege，不过那个是方法重载，不是重写
    public static void main(String[] args) {

        //内部类
        class  innerLamdaA implements LamdaFace< LamdaFace<Integer,Integer> , Integer > {
            //传进来的参数
            Integer  a ;
            //重写接口方法
            @Override
            public LamdaFace<Integer,Integer> lamdaMethod(Integer obj) {
                a = obj;
                //方法内部再建内部类
                class InnerLamdaB implements LamdaFace<Integer,Integer> {
                    Integer b ;
                    public Integer lamdaMethod(Integer obj) {
                        b = obj;
                        return a + b ;
                    }
                }
                //返回一个对象
                return  new InnerLamdaB() ;
            }
        }

        innerLamdaA FuncClass = new innerLamdaA();
        LamdaFace<Integer,Integer> func1 = FuncClass.lamdaMethod(1);
        Integer integer = func1.lamdaMethod(2);
        System.out.println( integer);
    }
}

interface  LamdaFace<R,In>{
    R lamdaMethod( In obj);
}
