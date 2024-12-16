package com.z01.StuSieve;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SieveMapper extends Mapper<Object, Text, IntWritable, Text> {

    private IntWritable studentStatus = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");  // 假设数据是逗号分隔的

        // 跳过标题行
        if (fields[0].equals("用户编码")) {
            return;
        }

        // 筛选所有年龄不是0岁的人的数据，17-30岁以外的，用户网龄大于网龄的，均不能是大学生，缴费用户最近一次缴费金额（元）为0的

        // 获取第四列的值（是否为大学生）

        int age = Integer.parseInt(fields[2]);
        int webAge = Integer.parseInt(fields[6]);
        float cost = Float.parseFloat(fields[8]);
        int isStudent = (17 <= age && age <=30) && (webAge <= age) && (cost >= 0.0) ? 1 : 0;
//        int isStudent = (webAge <= age) && (cost >= 0.0) ? 1 : 0;
        System.out.printf("%s,%d,%d,%f,%d\n",fields[0],age,webAge,cost,isStudent);
        // 设置输出的key为是否大学生的标志
        studentStatus.set(isStudent);

        // 输出是否大学生的标志和当前行数据
        context.write(studentStatus, value);
    }
}
