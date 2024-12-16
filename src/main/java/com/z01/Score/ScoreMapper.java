package com.z01.Score;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable, Text, Score, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields[0].equals("用户编码")) {
            return;
        }
        // 获取 Score 和 Age
        int score = Integer.parseInt(fields[29]);  // 最后一列（Score）
        int age = Integer.parseInt(fields[6]);    // 第七列（Age）

        // 创建自定义键
        Score customKey = new Score(score, age);

        // 将自定义键和整行数据作为输出
        context.write(customKey, value);
    }
}