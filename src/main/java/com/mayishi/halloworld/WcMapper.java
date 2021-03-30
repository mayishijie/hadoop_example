package com.mayishi.halloworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * 一个简单的wordcount
 *
 *  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *      KEYIN:输入的key的类型
 *      VALUEIN:值类型
 *      KEYOUT:输出的key类型
 *      VALUEOUT:输出的值类型
 * @author tty
 * @version 1.0 2021-03-30 21:58
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] s1 = s.split(" ");
        for (String s2 : s1) {
            k.set(s2);
            context.write(k,v);
        }
    }
}
