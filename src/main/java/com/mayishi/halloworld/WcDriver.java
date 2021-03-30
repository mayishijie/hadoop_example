package com.mayishi.halloworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author tty
 * @version 1.0 2021-03-30 22:13
 */
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "first wc program");

        // 2. 设置jar加载路径
        job.setJarByClass(WcDriver.class);

        // 3. 设置map和reduce,关联到job
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        // 4. 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5. 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path("/Users/tty/IdeaProjects/bigdata/hadoop_example/src/main/java/com/mayishi/halloworld/testFile/wc.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/tty/IdeaProjects/bigdata/hadoop_example/src/main/java/com/mayishi/halloworld/output"));


        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
