package strings.middle;

import timor.utils.Tuples2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @Author Timor
 * @Date 2023/12/21 10:14
 * @Version 1.0
 */
public class A1_无重复字符的最长子串 {

    public static void main(String[] args) {


        String s;
        s="abdefbcadadc";
        A1_无重复字符的最长子串 a = new A1_无重复字符的最长子串();
        //System.out.println(a.lengthOfLongestSubstring(s));

        System.out.println( a.lengthOfLongestSubstring_1(s) );


    }


    //暴力匹配法
    public int lengthOfLongestSubstring(String s) {
        char[] chars = s.toCharArray();

        int max_length = 0;
        HashSet<Character> set = new HashSet<>();

        for (int i = 0; i < chars.length; i++) {
            set.add(chars[i] );
           if(chars.length ==1) max_length =1;

            for( int j = i+1; j< chars.length;j++ ){
                if ( set.contains( chars[j] ) ) {
                    max_length =  max_length > (j-i) ? max_length:(j-i);
                    set.clear();
                    break;
                }
                set.add( chars[j] ) ;
                //收尾兜底,当到最后一次循环没有重复时,进行clear和把max_length标记上,注意这里的逻辑和上面不一样，要用j-i+1，不是上面的j-i
                //因为contains在上面，所以最后一个重复不会走到这里
                if(j == chars.length -1) {
                    max_length =  max_length > (j-i+1) ? max_length:(j-i+1);
                    set.clear();
                }

            }
        }
        return max_length;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    //自己优化，先相同数，划分区域块。 包含其他区域块的会被移除(无效的区域块)
    //TODO 这里逻辑有问题，再思考下
    public int lengthOfLongestSubstring_1(String s) {
        char[] chars = s.toCharArray();
        HashMap<Character, ArrayList<Integer>> map = new HashMap<>();


        for (int i = 0; i < chars.length; i++) {
            char  ch = chars[i];

            if ( !map.containsKey( ch ) ){
                ArrayList<Integer> list = new ArrayList<>();
                list.add(i);
                map.put(ch,list);
            }else {
                map.get(ch).add(i);
            }
        }

        HashSet<Tuples2<Integer,Integer> > set = new HashSet<>();


        for (ArrayList<Integer> list : map.values()) {
            list.sort( (a,b)->{ return a>b ? 1:0;} );
            for (int i = 0; i < list.size()-1 ; i++) {
                Tuples2<Integer, Integer> tuple = new Tuples2<>(list.get(i), list.get(i + 1));

                //TODO 这里逻辑有问题，再思考下,如果提前移除，会不会导致原本应该移除的，最后因提前移除，导致留下来了，好像也不会，这个是留下最小的，大的能移除的小的都能移除

                //记录本条是否插入
                boolean flag = true;
                for (Tuples2<Integer, Integer> filter : set) {
                    if( filter.f1()< tuple.f1() && filter.f2()>tuple.f2() ){
                        set.remove(filter);
                    }

                    //逻辑如果后进去的包裹住之前的某个区域,那么for循环不用进行了,因为前都执行过2个逻辑，不会漏掉
                    if(  filter.f2()>tuple.f1() && filter.f2()<tuple.f2() ){
                        flag =false;
                        break;
                    }
                }
                if(flag) { set.add( tuple ) ; }
            }
        }

        int max_num =0 ;
        Tuples2<Integer,Integer>  result = null ;

        for (Tuples2<Integer, Integer> need : set) {

            int length =  need.f2() -need.f1();
            if(result == null) {
                result = need ;
                max_num = length;
            }

            if( length> max_num ){
                result = need;
                max_num = length;
            }

        }
        return  max_num;
    }
}