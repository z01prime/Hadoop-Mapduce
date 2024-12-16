package com.z01.age_classifer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AgeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        // 对相同年龄段的所有值进行求和
        for (IntWritable val : values) {
            sum += val.get();
        }

        // 输出年龄段和该年龄段的总人数
        result.set(sum);
        context.write(key, result);
    }
}
