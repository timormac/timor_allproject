package summary.a1_envAndStream;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: A4_POJO
 * @Package: summary.a1_envAndStream
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/24 11:43
 * @Version:1.0
 */
public class A4_POJO {
    /**
     * #无参构造器
     * #字段为public或设置getter/setter
     * #字段为flink支持的类型
     * 字段必须是 Flink 支持类型：基本类型,集合类型, Pojo, Flink自定义Tuple
     * 满足以上,会自动识别为Pojo,不需要实现searizible
     */

    public  String name = "1";
    public  Map<String,String>  map = new HashMap<>();
    public  Tuple2<String,Integer>  tuple = new Tuple2<>("a",1);
    public  A5_InnerPojo   pojo = new A5_InnerPojo() ;
    public A4_POJO() {}

    @Override
    public String toString() {
        return super.toString() + name ;
    }
}
