package com.z01.StuPartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable studentStatus = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");  // 假设数据是逗号分隔的

        // 跳过标题行
        if (fields[0].equals("用户编码")) {
            return;
        }

        // 获取第四列的值（是否为大学生）
        int isStudent = Integer.parseInt(fields[3]);

        // 设置输出的key为是否大学生的标志
        studentStatus.set(isStudent);

        // 输出是否大学生的标志和当前行数据
        context.write(studentStatus, value);
    }
}
