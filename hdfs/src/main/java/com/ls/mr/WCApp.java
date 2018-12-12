package com.ls.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @Auther: ygy
 * @Date: 2018/12/12 21:36
 * @Description:
 */
public class WCApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String[] args2 = {"file:///d:/mr/", "file:///d:/mr/out"};
//        file:///d:/mr file:///d:/mr/out
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置job的各种属性
        job.setJobName("wcapp");
        job.setJarByClass(WCApp.class);
        job.setInputFormatClass(TextInputFormat.class);
        //设置输入路径
        FileInputFormat.addInputPath(job,new Path("file:///d:/mr/*"));
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path("file:///d:/mr/out") );
        //设置输出输入任务
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReduce.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.waitForCompletion(true);
    }
}
