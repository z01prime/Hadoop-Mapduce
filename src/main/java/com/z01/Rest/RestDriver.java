package com.z01.Rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RestDriver {
    public static void main(String[] args) throws Exception {
        // 配置作业
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "task6");
        job.setJarByClass(RestDriver.class);

        // 设置 Mapper 和 Reducer 类
        job.setMapperClass(RestMapper.class);
        job.setReducerClass(RestReducer.class);

        // 设置输出类型
        job.setOutputKeyClass(Rest.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
//        FileInputFormat.addInputPath(job, new Path("C:/Users/10562/Desktop/in/dataset.csv"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\10562\\Desktop\\out7"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
