package org.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class Delete {
    private static final String REMOTE  = "hdfs://192.168.56.118";
    private static  final String REMOTE_PATH2 = "/user/hadoop/dir/fio.png";
    private FileSystem client = null;

    @Before
    // 访问hdfs集群方式2：Configuration+程序中直接配置hdfs namenode
    public void set() throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", REMOTE);
        client = FileSystem.get(new URI(REMOTE), conf);
    }

    @Test
    public void delDirAndFileFromFS(){ // 删除目录及其中的文件
        try{
            client.delete(new Path(REMOTE_PATH2),true);
        } catch (Exception e){
            System.out.println(e);
        } finally {
            IOUtils.closeStream(client);
        }
    }

}
