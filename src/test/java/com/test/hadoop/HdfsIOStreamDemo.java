package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author: wzq
 * @create: 2020/12/04 19:12
 * @description: Java代码测试HDFS(使用IO流测试)
 */
public class HdfsIOStreamDemo {
    Configuration configuration = null;
    FileSystem fs = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://centos130:9000"), configuration, "root");
    }

    @After
    public void closeResource() throws IOException {
        fs.close();
    }

    /**
     * 使⽤IO流操作HDFS
     * 上传⽂件：准备输⼊流读取本地⽂件，使⽤hdfs的输出流写数据到hdfs
     * @throws IOException
     */
    @Test
    public void uploadFileIO() throws IOException {
        //1. 读取本地⽂件的输⼊流
        final FileInputStream inputStream = new FileInputStream(new
                File("D:/IOStream.txt"));
        //2. 准备写数据到hdfs的输出流
        final FSDataOutputStream outputStream = fs.create(new
                Path("/IOStream.txt"));
        // 3.输⼊流数据拷⻉到输出流 :数组的⼤⼩，以及是否关闭流底层有默认值
        IOUtils.copyBytes(inputStream, outputStream, configuration);
    }


    /**
     * 下载文件
     * @throws IOException
     */
    @Test
    public void getFileFromHDFS() throws IOException{
        // 1 获取输⼊流
        FSDataInputStream fis = fs.open(new Path("/IOStream.txt"));

        // 2 获取输出流
        FileOutputStream fos = new FileOutputStream(new
                File("D:/copyIOStream.txt.txt"));

        // 3 流的拷贝
        IOUtils.copyBytes(fis, fos, configuration);
    }

    // seek 定位读取
    @Test
    public void readFileSeek2() throws IOException{
        // 2 打开输⼊流,读取数据输出到控制台
        FSDataInputStream in = null;
        try{
            in= fs.open(new Path("/wcinput/wc.txt"));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0); //从头再次读取
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }

    }
}
