package difficult;

/**
 * @Title: Test
 * @Package: difficult
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/14 01:20
 * @Version:1.0
 */
public class a2_接雨水_别人做法 {


    public static void main(String[] args) {

        int[]  arr=   new int[]{3,2,1,1,1};

        System.out.println(  trap(  arr)  );

    }

    public static int trap(int[] height) {
        int sum = 0;
        int max = getMax(height);//找到最大的高度，以便遍历。
        for (int i = 1; i <= max; i++) {
            boolean isStart = false; //标记是否开始更新 temp
            int temp_sum = 0;
            for (int j = 0; j < height.length; j++) {
                if (isStart && height[j] < i) {
                    temp_sum++;
                }
                //这部代码，当按右遍历时,只有当右侧出现比当前行高的，才会执行累加 temp_sum操作，当右侧没有比当前行多的，不执行，过滤掉了
                if (height[j] >= i) {
                    sum = sum + temp_sum;
                    temp_sum = 0;
                    isStart = true;
                }
            }
        }
        return sum;
    }
    public static int getMax(int[] height) {
        int max = 0;
        for (int i = 0; i < height.length; i++) {
            if (height[i] > max) {
                max = height[i];
            }
        }
        return max;
    }

}
