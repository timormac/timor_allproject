package difficult;

import java.util.ArrayList;

/**
 * @Title: a_接雨水
 * @Package: difficult
 * @Description:
 * @Author: origin = height[i];
 * @Date: 2023/10/12 23:13
 * @Version:1.0
 */
public class a2_接雨水 {

/*    给定 n 个非负整数表示每个宽度为 1 的柱子的高度图，计算按此排列的柱子，下雨之后能接多少雨水。
    输入：height = [0,1,0,2,1,0,1,3,2,1,2,1]
    输出：6
    解释：上面是由数组 [0,1,0,2,1,0,1,3,2,1,2,1] 表示的高度图，在这种情况下，可以接 6 个单位的雨水（蓝色部分表示雨水）。*/

    public static void main(String[] args) {

        int[]  arr=   new int[]{3,2,1,1,1};

        System.out.println(  trap(  arr)  );




    }

    public static int trap(int[] height) {

        //找碗
        int origin = 0;
        int left =0;
        boolean switch1 = false;
        ArrayList<Integer>  list = new ArrayList<>();
        int highest_code=0;
        int highset_heigth=0;


        for( int i = 0; i<height.length; i++){

            if(height[i]>highset_heigth ){
                highest_code = i ;
                highset_heigth = height[i];
            }

        }

        System.out.println(highset_heigth + ":当前最高及坐标"+ highest_code );


        for( int i = 0; i<=highest_code; i++ ){

            if(!switch1){
                if (height[i] < origin)  {
                    switch1 = true ;
                    left = origin ;
                    list.add( i-1 );
                    System.out.println("找到碗左边界为"+(i-1) );
                }else{
                    origin = height[i];
                }
            }

            if (switch1){
                if( height[i] >= left ) {
                    switch1 = false ;
                    list.add(i);
                    System.out.println("找到碗右边界为"+i);
                    origin = height[i];
                }
            }

        }

        System.out.print("左碗边界数组为");
        for(Integer i:list) System.out.print(i+",");
        System.out.println();



        ArrayList<Integer[]>  list2 = new ArrayList<>();
        int a1 = 0;

        for(int i = 0 ; i < list.size(); i++){
            if(i%2 == 0 ) a1 = list.get(i);
            else  list2.add(  new Integer[]{a1, list.get(i)}  );

        }


        int result = 0 ;


        for (Integer[] integers : list2) {
            int all=  height[integers[0]]*( integers[1]-integers[0]-1  );
            int reduce = 0 ;
            for( int i = integers[0]+1; i <= integers[1]-1; i++ ){
                reduce+=height[i];
            }
            System.out.println("左边此碗面积为"+ (all - reduce) );
            result += all - reduce;

        }
        //此代码只适用于碗右边高度大于等于左边的情况,当左边大于右边缘时会漏掉

//===============右侧

        //找碗
        int origin_right = 0;
        int right =0;
        boolean switch_right = false;
        ArrayList<Integer>  list_right = new ArrayList<>();

        for( int i = height.length -1; i>= highest_code ; i-- ){

            if(!switch_right){
                if (height[i] < origin_right)  {
                    switch_right = true ;
                    right = origin_right ;
                    list_right.add( i+1 );
                    System.out.println("右碗起始坐标为"+(i+1));
                }else{
                    origin_right = height[i];
                }
            }

            if (switch_right){
                if( height[i] >= right ) {
                    switch_right = false ;
                    list_right.add(i);
                    origin_right = height[i];
                    System.out.println("右碗收尾坐标为"+i);
                }
            }

        }


        System.out.print("左碗边界数组为");
        for(Integer i:list_right) System.out.print(i+",");
        System.out.println();

        ArrayList<Integer[]>  list2_right = new ArrayList<>();

        int a1_right = 0;
        for(int i = 0 ; i < list_right.size(); i++){
            if(i%2 == 0 ) a1_right = list_right.get(i);
            else  list2_right.add(  new Integer[]{a1_right, list_right.get(i)}  );

        }

        for (Integer[] integers : list2_right) {
            int all=  height[integers[0]]*( integers[0]-integers[1]-1  );
            System.out.println( "all=" +all);
            int reduce = 0 ;
            for( int i = integers[1]+1; i <= integers[0]-1; i++ ){
                reduce+=height[i];
            }
            System.out.println("reduce="+reduce);
            System.out.println("左边此碗面积为"+ (all - reduce) );
            result += all - reduce;

        }




        return result;
    }


}
