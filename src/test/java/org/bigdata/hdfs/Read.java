package org.bigdata.hdfs;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.*; //需要导入，才能使用@Test
import sun.nio.ch.IOUtil;

public class Read {
    private static final String ADD  = "hdfs://192.168.56.118/user/hadoop/test/index.htm";

    static{
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    @Test
    public void readFromUrl() {

        InputStream in = null;
        try{
            // 这里的主机端口是core-site.xml中fs.defaultFS的值，没写端口就是80 https://blog.csdn.net/a_hui_tai_lang/article/details/81676429
            // 50070 是nn的web端口
            in = new URL(ADD).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e){
            System.out.println(e);
        } finally {
            IOUtils.closeStream(in);
        }
    }

    @Test
    public void readFromFS() {
        Configuration conf = new Configuration();
        InputStream inputStream = null;
        FileSystem fs = null;

        try{
            fs =  FileSystem.get(URI.create(ADD), conf);
            inputStream = fs.open(new Path(ADD));
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        }catch (Exception e){

        } finally {
            IOUtils.closeStream(inputStream);
        }
    }

    @Test
    public void readFromFSRandom() {
        Configuration conf = new Configuration();
        InputStream inputStream = null;
        FileSystem fs = null;

        try{
            fs =  FileSystem.get(URI.create(ADD), conf);
            inputStream = fs.open(new Path(ADD));
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
            ((FSDataInputStream) inputStream).seek(0);
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        }catch (Exception e){

        } finally {
            IOUtils.closeStream(inputStream);
        }
    }
}
