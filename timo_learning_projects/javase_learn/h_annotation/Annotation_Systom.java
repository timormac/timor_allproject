package h_annotation;

/**
 * @Title: Annotation_Systom
 * @Package: h_annotation
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/15 17:59
 * @Version:1.0
 */

//注解只能用在类中，修饰属性和方法，不能用在方法的变量
class Annotation_Systom {
    void methondMustWrite(){};

    @Deprecated
    static void methodOutDated(){
        System.out.printf("过时了");
    };

    public static void main(String[] args) {

        methodOutDated();
    }

}
