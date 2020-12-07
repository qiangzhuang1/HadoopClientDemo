package com.test.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author: wzq
 * @create: 2020/12/04 19:12
 * @description: Java代码测试HDFS(使用HDFS封装好的代码测试)
 */
public class HdfsClientDemo {
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
     * 创建一个目录
     * @throws IOException
     */
    @Test
    public void mkdirs() throws IOException {
        fs.mkdirs(new Path("/javaTest"));
    }


    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void copyFromLocalToHdfs() throws IOException {
        //src:源⽂件⽬录：本地路径
        //dst:⽬标⽂件⽬录，hdfs路径
        fs.copyFromLocalFile(new Path("D:/code.txt"),new Path("/javaTest"));
    }

    //下载⽂件
    @Test
    public void copyFromHdfsToLocal() throws IOException{
        // boolean:是否删除源⽂件
        //src:hdfs路径
        //dst:⽬标路径，本地路径
        fs.copyToLocalFile(true, new Path("/javaTest/code.txt"), new Path("D:/code_copy.txt"));
    }

    /**
     * 删除文件或目录
     */
    @Test
    public void del() throws IOException {
        fs.delete(new Path("/1"), true);
    }

    //遍历hdfs的根目录得到文件夹以及文件夹的信息（名称，权限，长度等）
    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus next = remoteIterator.next();
            System.out.println("长度:" + next.getLen());
            System.out.println("名称:" + next.getPath().getName());
            System.out.println("权限:" + next.getPermission());
            System.out.println("用户:" + next.getOwner());
            System.out.println("分组:" + next.getGroup());
            // 块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("主机名称:" + host);
                }
            }
            System.out.println("------------------------------");
        }
    }

    // 文件夹以及文件的判断
    @Test
    public void isFile() throws IOException {
        final FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            final boolean flag = fileStatus.isFile();
            if (flag) {
                System.out.println("⽂件:" + fileStatus.getPath().getName());
            } else {
                System.out.println("⽂件夹:" + fileStatus.getPath().getName());
            }
        }
    }

    /**
     * 验证packet
     * @throws IOException
     */
    @Test
    public void testPacket() throws IOException {
        //1. 读取本地⽂件的输⼊流
        final FileInputStream inputStream = new FileInputStream(new
                File("D:/testPacket2.txt"));
        //2. 准备写数据到hdfs的输出流
        final FSDataOutputStream outputStream = fs.create(new Path("/testPacket3.txt"), new Progressable() {
            public void progress() {
                System.out.println("传输一次64KB");
            }
        });
        // 3. 实现流拷⻉;默认关闭流选项是true，所以会⾃动关闭
        IOUtils.copyBytes(inputStream, outputStream, configuration);
    }
}
