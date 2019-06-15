package org.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;

public class NewAccessWayToWrite {
    FileSystem client = null;

    @Before
    // 访问hdfs集群方式1：Configuration+自动从资源文件夹加载core-site.xml配置文件
    public void set() throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        client = FileSystem.get( conf);
    }

    @Test
    public void readFile() throws Exception{
        FSDataInputStream inputStream = client.open(new Path("/user/hadoop/test/index.htm"));
        IOUtils.copyBytes(inputStream, System.out, 1024, false);
        IOUtils.closeStream(inputStream);
    }

    @Test
    public  void writeFile() throws Exception{
        FSDataOutputStream outputStream = client.create(new Path("/user/hadoop/dir/newFile.txt"), true, 1024, (short)2,1048576);
        ByteArrayInputStream bi =  new ByteArrayInputStream("Today is a fine day".getBytes());
        IOUtils.copyBytes(bi, outputStream, 1024, true);
    }

}
