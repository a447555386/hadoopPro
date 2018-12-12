package com.ls.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Auther: ygy
 * @Date: 2018/12/12 21:30
 * @Description:
 */
public class WCReduce extends Reducer<Text,IntWritable,Text,IntWritable> {
    protected void reduce(Text keys,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable iw:
             values) {
            count=count + iw.get();
        }
        context.write(keys,new IntWritable(count) );
    }
}
