package h_annotation;

/**
 * @Title: MyApp
 * @Package: h_annotation
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 18:59
 * @Version:1.0
 */
public class MyApp {
    @CustomValue("custom.property.key")
    private String customPropertyValue;
    public void init() {
        String url="/Users/timor/Desktop/git_warehouse/timo_learning_projects/javase_learn/h_annotation/custom.properties";
        CustomValueProcessor processor = new CustomValueProcessor(url);

        //这里传递的this是对象本身，在代码里对传进来的this做了.getClass
        processor.process(this);
        System.out.println(customPropertyValue);
    }

    public static void main(String[] args) {

        MyApp myApp = new MyApp();
        myApp.init();


    }
}
