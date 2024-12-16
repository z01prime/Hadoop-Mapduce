package com.z01.Score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ScoreDriver {
    public static void main(String[] args) throws Exception {
        // 配置作业
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task4");
        job.setJarByClass(ScoreDriver.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Score.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
//        FileInputFormat.addInputPath(job, new Path("C:\\Users\\10562\\Desktop\\out3\\part-r-00000"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\10562\\Desktop\\out4"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}