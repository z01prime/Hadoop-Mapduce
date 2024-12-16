package com.z01.age_classifer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private IntWritable age = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // 假设输入数据每一行是一个用户的信息，用户年龄是第三列
        // 按照实际情况修改此部分逻辑，如果输入格式不同，可能需要处理更多字段

        String[] fields = value.toString().split(",");  // 假设数据是逗号分隔的
        if (fields[0].equals("用户编码")) {
            return;
        }
        int temp_age = Integer.parseInt(fields[2]);  // 年龄在第三列

        age.set(temp_age);

        // 输出年龄段和计数（1）
        context.write(age, one);
    }
}


