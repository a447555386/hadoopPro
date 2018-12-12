package com.ls.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;


/**
 * @Auther: ygy
 * @Date: 2018/11/8 23:11
 * @Description:测试hdfs
 */
public class TestHdfs {

    @Test
    public void testSave() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("hdfs://s10/usr/centos/hello.txt");
        FSDataInputStream fsDataInputStream = fs.open(path);
        ByteArrayOutputStream fos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fsDataInputStream,fos,1024);
        fsDataInputStream.close();
        System.out.println(new String(fos.toByteArray()));
    }

    @Test
    public void testWrite() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/usr/centos/a.txt");
//        FSDataOutputStream fos = fs.create(path);
        FSDataOutputStream fos = fs.create(path,true,1024, (short) 2,5);

        fos.write("how are you".getBytes());
        fos.close();
    }

}
