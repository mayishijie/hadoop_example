package com.mayishi.customseri;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author tty
 * @version 1.0 2021-03-31 09:48
 */
public class PhReduce extends Reducer<Text, PhoneBean, Text, PhoneBean> {
    @Override
    protected void reduce(Text key, Iterable<PhoneBean> values, Context context) throws IOException, InterruptedException {
        long up_sum = 0L;
        long down_sum = 0L;
        long num = 0;
        for (PhoneBean value : values) {
            long upFlow = value.getUpFlow();
            long downFlow = value.getDownFlow();
            up_sum += upFlow;
            down_sum += downFlow;
            num += 1;
        }
        PhoneBean phoneBean = new PhoneBean(up_sum, down_sum,num);
        context.write(key, phoneBean);
    }
}
