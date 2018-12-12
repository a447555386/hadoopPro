package com.ls.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Auther: ygy
 * @Date: 2018/12/12 21:07
 * @Description:
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text keyout = new Text();
        IntWritable valueout = new IntWritable();
        String[] strings = value.toString().split(" ");
        for (String string:
             strings) {
            keyout.set(string);
            valueout.set(1);
            context.write(keyout,valueout );
        }

    }
}
