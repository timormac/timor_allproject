package middle;

/**
 * @Title: 两数相加
 * @Package: middle
 * @Description:
 * @Author: lpc
 * @Date: 2023/12/17 20:00
 * @Version:1.0
 */
public class 两数相加 {

    public static void main(String[] args) {
        两数相加 a = new 两数相加();
        ListNode listNode = a.addTwoNumbers(new ListNode(8,new ListNode(8)),new ListNode(4,new ListNode(6,new ListNode(3) )) );
        ListNode tmp = listNode;
        while ( tmp!=null ){
            System.out.print(tmp.val);
            tmp = tmp.next;

        }
    }
    public ListNode addTwoNumbers(ListNode l1, ListNode l2) {

        ListNode result_node = new ListNode();
        ListNode current_result_node = result_node;
        ListNode current_l1 = l1;
        ListNode current_l2 = l2;
        int num ;
        int next_sum = 0;
        int result_num;
        int num_current_l1;
        int num_current_l2;

        while (!(current_l1 == null && current_l2 == null && next_sum == 0)) {

            if (current_l1 != null) num_current_l1 =current_l1.val;
            else  num_current_l1 = 0;

            if (current_l2 != null)  num_current_l2 =current_l2.val;
            else num_current_l2 = 0  ;

            num = num_current_l1+ num_current_l2 + next_sum;
            next_sum = num/10;
            result_num = num%10;
            current_result_node.val = result_num;

            if(current_l1 != null)  current_l1 = current_l1.next;
            if(current_l2 != null)  current_l2 = current_l2.next;

            if(  current_l1!=null || current_l2!=null || ( current_l1==null && current_l2==null && next_sum>0 )  ){
                current_result_node.next = new ListNode();
                current_result_node  = current_result_node.next;
            }

        }

        return result_node;

    }
}

class ListNode {
    int val;
    ListNode next;
    ListNode() {}
    ListNode(int val) { this.val = val; }
    ListNode(int val, ListNode next) { this.val = val; this.next = next; }
}


