package strings.middle;

import timor.utils.Tuples2;

import java.util.*;

/**
 * @Author Timor
 * @Date 2023/12/21 10:14
 * @Version 1.0
 */
public class A1_无重复字符的最长子串 {

    public static void main(String[] args) {


        String s;
       // s="abdefbcadadc";
       // s= "abcabcbb";
        s = "a";
        //s = "aaa";
        A1_无重复字符的最长子串 a = new A1_无重复字符的最长子串();
        //System.out.println(a.lengthOfLongestSubstring(s));

        System.out.println( a.lengthOfLongestSubstring_2(s) );


    }


    //TODO 暴力匹配法
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

    //TODO 官方的滑动窗口
    /**具体思路如下
     * 案例:abecfca
     * 第一步从首字母开始开窗,找到最长的无重复串   ｜abecf｜cadc
     * 记录长度,然后移动左指针到第一个重复字符+1，abec｜f｜cadcmn，,然后右指针后移，找到最长无重复串
     * 一次for循环遍历就行结束，也不用嵌套
     */
    public int lengthOfLongestSubstring_2(String s) {
        char[] chars = s.toCharArray();
        int max_length = 0;
        Map<Character,Integer> map = new LinkedHashMap<>();
        int pinBegin =0 ;

        for (int i = 0; i < chars.length;i++ ) {
            char ele = chars[i];
            if( !map.keySet().contains( ele ) ){
                map.put( ele ,i );
                if( i == chars.length-1 ){
                    max_length = Math.max( max_length , i-pinBegin+1 );
                }
            }else {
                //记录最大长度
                max_length = Math.max(max_length, i - pinBegin);
                //把keyset重复元素前的元素移除，然后跳指针到重复元素位置+1
                int tmp =  map.get(ele)+1 ;
                for (int t = pinBegin ; t < tmp ; t++) {
                    map.remove( chars[t] );
                }
                //左指针后移动
                pinBegin = tmp;
                map.put( ele,i ) ;
            }
        }
        return max_length;
    }





    //自己优化，先相同数，划分区域块。 包含其他区域块的会被移除(无效的区域块)
    //这垃圾方法放弃了，要控制的太多，逻辑也不清晰
    public int lengthOfLongestSubstring_1(String s) {

        char[] chars = s.toCharArray();
        HashMap<Character, ArrayList<Integer>> map = new HashMap<>();


        //第一个重复节点
        int first_node = 1000000;
        //最后一个重复节点
        int last_node =0;
        int total = 0 ;
        int kk = 0;


        for (int i = 0; i < chars.length; i++) {
            char  ch = chars[i];
            if ( !map.containsKey( ch ) ){
                ArrayList<Integer> list = new ArrayList<>();
                list.add(i);
                map.put(ch,list);
                total +=1;

            }else {
                map.get(ch).add(i);
                last_node = i > last_node ? i:last_node;
                first_node = map.get(ch).get(0)<first_node ? map.get(ch).get(0):first_node;
                kk +=1;
            }
        }


        HashSet<Tuples2<Integer,Integer> > set = new HashSet<>();

        for (ArrayList<Integer> list : map.values()) {
            list.sort( (a,b) -> a>b ? 1:0 );
            for (int i = 0; i < list.size()-1 ; i++) {
                Tuples2<Integer, Integer> tuple = new Tuples2<>(list.get(i), list.get(i + 1));

                //记录本条是否插入
                boolean flag = true;
                for (Tuples2<Integer, Integer> filter : set) {
                    //移除那些范围完全覆盖，当先指针元素的
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

        System.out.println("第一节点"+first_node);
        System.out.println("第二节点"+last_node);

        first_node = first_node+1;
        last_node = chars.length -last_node ;

        int max_num = first_node>last_node ? first_node:last_node;


        if (kk==0){
            max_num =  chars.length;
        }
        System.out.println(max_num);

        for (Tuples2<Integer, Integer> need : set) {
            int length =  need.f2() -need.f1();
            max_num = max_num>length ?max_num :length;
        }

        return  max_num;
    }
}