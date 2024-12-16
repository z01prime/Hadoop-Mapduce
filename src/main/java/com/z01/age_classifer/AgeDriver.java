package com.z01.age_classifer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeDriver {

    public static void main(String[] args) throws Exception {
        // 配置Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task1");
        job.setJarByClass(AgeDriver.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);

        // 设置输出键值对类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入输出路径
//        FileInputFormat.addInputPath(job, new Path("C:\\Users\\10562\\Desktop\\in\\dataset.csv"));  // 输入路径
//        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\10562\\Desktop\\out1"));  // 输出路径

        FileInputFormat.addInputPath(job, new Path(args[0]));  // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 输出路径

        // 提交任务并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}






