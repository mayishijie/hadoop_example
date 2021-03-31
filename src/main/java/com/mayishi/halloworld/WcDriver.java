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
 * 提交命令: hadoop jar mr程序jar包 driver类 输入路径   输出路径
 * hadoop jar  wc.jar
 *  com.mayishi.halloworld.WcDriver /user/mayishijie/input /user/mayishijie/output
 * @author tty
 * @version 1.0 2021-03-30 22:13
 */
public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        //没有指定时,默认本地执行
        ////指定默认文件系统
        //conf.set("fs.defaultFS","hdfs://mayi101:8020");
        ////The runtime framework for executing MapReduce jobs. Can be one of local, classic or yarn.
        //conf.set("mapreduce.framework.name", "yarn");
        ////If enabled, user can submit an application cross-platform i.e. submit an application from a Windows client to a Linux/Unix server or vice versa.
        //conf.set("mapreduce.app-submission.cross-platform", "true");
        ////The hostname of the RM.
        //conf.set("yarn.resourcemanager.hostname","mayi101");

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
        FileInputFormat.setInputPaths(job,new Path("/Users/tty/IdeaProjects/bigdata/hadoop_example/src/main/resources/testFile/wc.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/tty/IdeaProjects/bigdata/hadoop_example/src/main/resources/testFile/output"));


        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
