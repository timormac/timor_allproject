package a1_collections;

import com.google.common.collect.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Title: a1_collections.A1_CollectionGuava
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/18 16:42
 * @Version:1.0
 */
public class A1_CollectionGuava {

    public static void main(String[] args) {

        //连接：https://www.kancloud.cn/wizardforcel/java-opensource-doc/112614


        List<String> arrayList = Lists.newArrayList("apple", "banana", "orange");
        System.out.println(arrayList);

        Set<Integer> hashSet = Sets.newHashSet(1, 2, 3, 4, 5);
        System.out.println(hashSet);

        Map<String, Integer> hashMap = Maps.newHashMap();
        hashMap.put("apple", 1);
        hashMap.put("banana", 2);
        hashMap.put("orange", 3);
        System.out.println(hashMap);

        Multiset<String> multiset = HashMultiset.create();
        multiset.add("apple");
        multiset.add("banana");
        multiset.add("banana");
        multiset.add("orange");
        System.out.println(multiset);

        Multimap<String, Integer> multimap = ArrayListMultimap.create();
        multimap.put("fruit", 1);
        multimap.put("fruit", 2);
        multimap.put("fruit", 3);
        multimap.put("color", 4);
        System.out.println(multimap);


    }

}
