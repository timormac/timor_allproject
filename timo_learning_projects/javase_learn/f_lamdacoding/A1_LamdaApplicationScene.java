package f_lamdacoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Title: A1_LamdaApplicationScene
 * @Package: f_lamdacoding
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/7 18:20
 * @Version:1.0
 */
public class A1_LamdaApplicationScene {

    public static void main(String[] args) {

        ArrayList<String> arr = new ArrayList<>();
        ArrayList<String> arr2 = new ArrayList<>();
        arr.add("a");
        arr2.add("b");

        //————————————————————————————遍历集合——————————————————————————————

        //遍历集合 .forEach(lamda表达式)
        arr.forEach ( str -> System.out.println(str) );
        //lamda写法2:参数只用一次,传给方法体时省略参数, ::然后不加()。含义：把for的变量传给，arr2的add方法里
        arr.forEach( arr2::add);
        //lamda写法3:调用参数本身的方法时，写法: 参数类型::方法
        arr.stream().map( String::length );

        // Stream用法:先过滤a开头，然后全部大写,再转list
        arr.stream().filter( s->s.startsWith("a") ).map( String::toUpperCase).collect(Collectors.toList());

        //stream流:把list按指定key分组
        Map<Character, List<String>> collect = arr.stream().collect(Collectors.groupingBy(s -> s.toCharArray()[0]));



    }
}
