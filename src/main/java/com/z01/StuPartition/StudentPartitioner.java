package com.z01.StuPartition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StudentPartitioner extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
        // 获取第四列的值（是否为大学生）
        int isStudent = key.get();

        // 0 和 1 分为两个不同的分区
        if (isStudent == 0) {
            return 0;  // 非大学生分到第一个分区
        } else {
            return 1;  // 大学生分到第二个分区
        }
    }
}
