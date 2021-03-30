package com.mayishi.halloworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author tty
 * @version 1.0 2021-03-30 22:09
 */
public class WcReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    IntWritable v = new IntWritable();
    /**
     * 按照key聚合过来的所有value做聚合处理
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        //输出
        context.write(key, v);
    }
}
