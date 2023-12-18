package com.a3_serialazable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Title: UserSerialazeDao
 * @Package: com.a3_serialazable
 * @Description:
 * @Author: lpc
 * @Date: 2023/10/17 19:30
 * @Version:1.0
 */
public class UserSerializeDao implements Writable {

    String  companyId;
    String   user;
    Integer  count;
    Double    money;

    Integer  sum_count=0;
    Double   sum_money=0.0;

    public UserSerializeDao() {
    }

    public UserSerializeDao(String companyId,String user, Integer count, Double money) {
        this.companyId=companyId;
        this.user = user;
        this.count = count;
        this.money = money;
    }

    public void count_sum(UserSerializeDao user){
        this.sum_count = this.count+user.count;
    }

    public void money_sum(UserSerializeDao user){
        this.sum_money=this.money+user.money;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(companyId);
        dataOutput.writeUTF(user);
        dataOutput.writeInt(count);
        dataOutput.writeDouble(money);
        dataOutput.writeInt(sum_count);
        dataOutput.writeDouble(sum_money);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        companyId = dataInput.readUTF();
        user  = dataInput.readUTF();
        count = dataInput.readInt();
        money = dataInput.readDouble();
        sum_count = dataInput.readInt();
        sum_money = dataInput.readDouble();

    }

    @Override
    public String toString() {
       return "\t"+ this.sum_money+"\t"+this.sum_count  ;
    }

}
