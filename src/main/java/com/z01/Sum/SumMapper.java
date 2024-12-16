package com.z01.Sum;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class SumMapper extends Mapper<LongWritable, Text, Sum, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].equals("用户编码")) {
            return;
        }
        // 提取第23到26列（0索引为例，第22到25列）
        int col23 = Integer.parseInt(fields[22]);
        int col24 = Integer.parseInt(fields[23]);
        int col25 = Integer.parseInt(fields[24]);
        int col26 = Integer.parseInt(fields[25]);

        // 计算 Sum
        int sum = col23 + col24 + col25 + col26;

        // 创建自定义键
        Sum customKey = new Sum(sum);

        // 输出自定义键和原始数据
        context.write(customKey, value);
    }
}
