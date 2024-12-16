package com.z01.Score;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<Score, Text, Text, NullWritable> {
    @Override
    protected void reduce(Score key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 对每个 key 输出一次，因为 sort 阶段已经按照顺序输出了
        for (Text value : values) {
            context.write(value, NullWritable.get());
        }
    }
}