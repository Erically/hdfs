package org.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class Status {
    private static final String REMOTE  = "hdfs://192.168.56.118";
    // 重要：
    // 1.REMOTE+REMOTE_PATH3和纯REMOTE_PATH3效果是一样的
    // 2.new Configuration()之后，直接使用，是使用依赖包自带的配置文件，其指定的是本地文件系统，适用于standalone集群的开发测试
    // 3.也可以new Configuration()之后，在代码里面指定配置，如下，适用于集群的开发测试
    // 4.也可以将配置文件从集群拷贝出来，放在代码的resources中，new Configuration()实例，将会自动去加载，更适合集群的开发测试
    private static final String REMOTE_PATH3 = "/user/hadoop/test/index.htm";
    private FileSystem client = null;

    @Before
    public void set() throws Exception{
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", REMOTE);
        client = FileSystem.get(new URI(REMOTE), conf);
    }


    @Test
    public void lsFS(){
        try {
            // 获取一个文件或目录的状态元数据
//            FileStatus fileStatus = client.getFileStatus(new Path(REMOTE_PATH3));
//            System.out.println("file owner:"+fileStatus.getOwner());

            // 获取本目录下的多个文件或目录的状态状态元数据，不会递归下去
            FileStatus[] fileStatuses = client.listStatus(new Path("/user/hadoop"));
            for(FileStatus f: fileStatuses){
                System.out.println("file path:"+f.getPath()+"|owner:"+f.getOwner());
            }

            client.listFiles(new Path("/"), true);

        } catch (Exception e){
            System.out.println(e);
        } finally {

        }
    }
}
