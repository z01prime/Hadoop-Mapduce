package com.z01.StuPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PartitionDriver {

    public static void main(String[] args) throws Exception {
        // 配置Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task2");
        job.setJarByClass(PartitionDriver.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        // 设置输出键值对类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置分区器
        job.setPartitionerClass(StudentPartitioner.class);
        job.setNumReduceTasks(2);

        // 设置输入输出路径
//        FileInputFormat.addInputPath(job, new Path("C:/Users/10562/Desktop/in/dataset.csv"));
//        FileOutputFormat.setOutputPath(job, new Path("C:/Users/10562/Desktop/out"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交任务并等待完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
