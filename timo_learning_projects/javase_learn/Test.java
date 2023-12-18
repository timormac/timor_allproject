import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @Title: Test
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/7 19:50
 * @Version:1.0
 */
public class Test {

    public static void main(String[] args) {
        ArrayList<String> arr = new ArrayList<>();
        ArrayList<String> arr2 = new ArrayList<>();
        arr.add("a");

        for (String s : arr) {
            if( s.startsWith("a") ){
                String n = s.toUpperCase();
                arr2.add(n);
            }
        }


        Map<Character, List<String>> collect = arr.stream().collect(Collectors.groupingBy(s -> s.toCharArray()[0]));


        //先过滤a开头，然后全部大写
        arr.stream().filter( s->s.startsWith("a") ).map( String::toUpperCase);


    }
}
