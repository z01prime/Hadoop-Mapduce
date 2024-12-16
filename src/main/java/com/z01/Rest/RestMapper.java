package com.z01.Rest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RestMapper extends Mapper<LongWritable, Text, Rest, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].equals("用户编码")) {
            return;
        }
        // 提取第23到26列（0索引为例，第22到25列）
        int col12 = Integer.parseInt(fields[11]);
        if (col12 >= 50) return;
        // 创建自定义键
        Rest customKey = new Rest(col12);

        // 输出自定义键和原始数据
        context.write(customKey, value);
    }
}