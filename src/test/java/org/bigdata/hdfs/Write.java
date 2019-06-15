package org.bigdata.hdfs;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import java.io.*;

public class Write {
    private static final String REMOTE  = "hdfs://192.168.56.118";
    private static  final String REMOTE_PATH = "/user/hadoop/huqiwei";
    private static  final String REMOTE_PATH2 = "/user/hadoop/dir/fio2.png";
    private static final String REMOTE_PATH3 = "/user/hadoop/test/index.htm";
    private static final String LOC = "C:\\Users\\YAYA\\Desktop\\fio.png";


    @Test
    public void mkdirsToFS(){
        FileSystem client = null;
        try {
            // 用户权限不够将不能创建目录和文件
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", REMOTE);
            client = FileSystem.get(conf);
            client.mkdirs(new Path(REMOTE_PATH));
        } catch (Exception e){
            System.out.println(e);
        } finally {
//            IOUtils.closeStream(inputStream);
//            IOUtils.closeStream(outputStream);
            IOUtils.closeStream(client);

        }

    }

    @Test
    public void putFileToFS(){
        FileSystem client = null;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = new BufferedInputStream(new FileInputStream(LOC));
            // 用户权限不够将不能创建目录和文件
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", REMOTE);
            client = FileSystem.get(conf);
            // create没有目录，将会创建这个目录
            outputStream = client.create(new Path(REMOTE_PATH2), new Progressable() {
                public void progress() {
                    System.out.println(".");
                }
            });

            IOUtils.copyBytes(inputStream, outputStream, 800*1024, false);
        } catch (Exception e){
            System.out.println(e);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }

    @Test
    public void appendToFSFile(){
        FileSystem client = null;
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            // 用户权限不够将不能创建目录和文件
            System.setProperty("HADOOP_USER_NAME", "hadoop");
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", REMOTE);
            client = FileSystem.get(conf);
            // create没有目录，将会创建这个目录
            outputStream = client.append(new Path(REMOTE_PATH3));
            inputStream = new BufferedInputStream( new ByteArrayInputStream("append".getBytes()));
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
        } catch (Exception e){
            System.out.println(e);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }
}
