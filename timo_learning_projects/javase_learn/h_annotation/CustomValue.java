package h_annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Title: CustomValue
 * @Package: h_annotation
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/6 18:56
 * @Version:1.0
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface CustomValue {
    String value();

    //代码中是这样给注解赋予功能的
    // CustomValue anno = field.getAnnotation(CustomValue.class);
    // String propertyName = anno.value();
}

