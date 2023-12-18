package g_generictype;

import java.util.HashMap;
import java.util.Map;

/**
 * @Title: ApplicationRangeType
 * @Package: g_rangetype
 * @Description:
 * @Author: XXX
 * @Date: 2023/5/15 16:37
 * @Version:1.0
 */
public class CollectionsGenericType {


    public static void main(String[] args) {

        //范型应用在集合中, 注意因为有类型推断，所以在new HashMap的时候<>里可以不写，因为前面写了
        Map<String,Integer>  map = new HashMap<>() ;


    }



}
