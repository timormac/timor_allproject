package h_annotation;

/**
 * @Title: CustomValueProcessor
 * @Package: h_annotation
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 18:58
 * @Version:1.0
 */
import java.lang.reflect.Field;

public class CustomValueProcessor {
    private CustomProperties customProperties;

    public CustomValueProcessor(String filePath) {
        customProperties = new CustomProperties(filePath);
    }


    public void process(Object obj) {
        //调用时把this对象传进来了
        Class<?> clazz = obj.getClass();
        //遍历对象中所有属性
        for (Field field : clazz.getDeclaredFields()) {
            //查看遍历到的当前属性是否有CustomValue注解
            CustomValue customValueAnnotation = field.getAnnotation(CustomValue.class);

            //如果有注解那么对注解赋值
            if (customValueAnnotation != null) {
                String propertyName = customValueAnnotation.value();
                //读取到了文件中的值
                String propertyValue = customProperties.getProperty(propertyName);
                if (propertyValue != null) {
                    field.setAccessible(true);
                    try {
                        //obj是传来来的MyApp对象
                        field.set(obj, propertyValue);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}