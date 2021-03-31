package com.mayishi.customseri;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * @author tty
 * @version 1.0 2021-03-31 09:42
 */
public class PhMapper extends Mapper<LongWritable, Text,Text,PhoneBean> {
    Text k = new Text();
    PhoneBean v = new PhoneBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        k.set(split[0]);
        long up = Long.parseLong(split[1]);
        long dow = Long.parseLong(split[2]);
        long sum = up + dow;
        v.setUpFlow(up);
        v.setDownFlow(dow);
        v.setSumFlow(sum);
        context.write(k, v);
    }
}
