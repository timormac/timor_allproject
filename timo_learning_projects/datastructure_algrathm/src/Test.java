import java.util.*;

/**
 * @Title: Test
 * @Package: PACKAGE_NAME
 * @Description:
 * @Author: lpc
 * @Date: 2023/11/30 10:20
 * @Version:1.0
 */
public class Test {

//
//    如果可以使用以下操作从一个字符串得到另一个字符串，则认为两个字符串 接近 ：
//
//    操作 1：交换任意两个 现有 字符。
//    例如，abcde -> aecdb
//    操作 2：将一个 现有 字符的每次出现转换为另一个 现有 字符，并对另一个字符执行相同的操作。
//    例如，aacabb -> bbcbaa（所有 a 转化为 b ，而所有的 b 转换为 a ）
//    你可以根据需要对任意一个字符串多次使用这两种操作。
//
//    给你两个字符串，word1 和 word2 。如果 word1 和 word2 接近 ，就返回 true ；否则，返回 false 。


    public static void main(String[] args) {

        System.out.println(closeStrings("cabbba","abbccc"));


    }


    public static boolean closeStrings2(String word1, String word2) {
        int[] count1 = new int[26], count2 = new int[26];
        for (char c : word1.toCharArray()) {
            count1[c - 'a']++;
        }
        for (char c : word2.toCharArray()) {
            count2[c - 'a']++;
        }
        for (int i = 0; i < 26; i++) {
            if (count1[i] > 0 && count2[i] == 0 || count1[i] == 0 && count2[i] > 0) {
                return false;
            }
        }
        Arrays.sort(count1);
        Arrays.sort(count2);
        return Arrays.equals(count1, count2);
    }



    public static boolean closeStrings(String word1, String word2) {

        char[] chars1 = word1.toCharArray();
        char[] chars2 = word2.toCharArray();

        HashMap<Character, Integer> map1 = new HashMap<>();
        HashMap<Character, Integer> map2 = new HashMap<>();

        for (char c : chars1) {
            if(!map1.containsKey(c))  map1.put(c,1);
            else map1.put(c,map1.get(c)+1);
        }

        for (char c : chars2) {
            if(!map2.containsKey(c))  map2.put(c,1);
            else map2.put(c,map2.get(c)+1);
        }

        Set<Character> set1 = map1.keySet();
        Set<Character> set2 = map2.keySet();
        ArrayList<Integer> arr1 = new ArrayList<>();
        ArrayList<Integer> arr2 = new ArrayList<>();

        boolean flag=true;


        for (Character character : set1) {
            arr1.add(map1.get(character));
           if(!set2.contains(character))  flag=false;
        }

        for (Character character : set2) {
            arr2.add(map2.get(character));
            if(!set1.contains(character))  flag=false;
        }

        if(!flag) return false;

        arr1.sort( (a1,a2)->{
            if(a1>a2) return 0;
            else return 1;
        });




        arr2.sort( (a1,a2)->{
            if(a1>a2) return 0;
            else return 1;
        });

        for (Integer integer : arr1) {
            System.out.print(integer);
            System.out.println("a1---");
        }

        for (Integer integer : arr2) {
            System.out.print(integer);
            System.out.println("a2----");
        }



        System.out.println(flag);

        for (int i = 0; i < arr1.size(); i++) {
            Integer i1 = arr1.get(i);
            Integer i2 = arr2.get(i);
            if(!i1.equals(i2)) flag=false;
            System.out.println(flag);
        }

        return flag;

    }
}
