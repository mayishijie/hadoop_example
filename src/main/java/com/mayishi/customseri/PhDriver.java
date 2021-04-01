package com.mayishi.customseri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义序列化
 * @author tty
 * @version 1.0 2021-03-31 09:57
 */
public class PhDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ph");

        job.setJarByClass(PhDriver.class);

        job.setMapperClass(PhMapper.class);
        job.setReducerClass(PhReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneBean.class);


        //job.setNumReduceTasks(5);

        FileInputFormat.setInputPaths(job, new Path("/Users/tty/IdeaProjects/bigdata/hadoop_example/src/main/resources/testFile/ph.txt"));
        FileOutputFormat.setOutputPath(job,new Path("/Users/tty/IdeaProjects/bigdata/hadoop_example/src/main/resources/output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);

    }
}
