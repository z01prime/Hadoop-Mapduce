package com.z01.Sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumDriver {
    public static void main(String[] args) throws Exception {
        // 配置作业
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task5");
        job.setJarByClass(SumDriver.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Sum.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
//        FileInputFormat.addInputPath(job, new Path("C:\\Users\\10562\\Desktop\\out4\\part-r-00000"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\10562\\Desktop\\out5"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

