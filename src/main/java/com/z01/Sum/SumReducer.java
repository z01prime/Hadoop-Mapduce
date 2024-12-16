package com.z01.Sum;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;

public class SumReducer extends Reducer<Sum, Text, Text, NullWritable> {
    @Override
    protected void reduce(Sum key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 对每个 key 输出一次，因为 map 阶段已经按照顺序输出了
        for (Text value : values) {
            // 你可以在输出时附加 Sum（从 key 中获取）
            context.write(new Text(value.toString() + "," + key.toString()), NullWritable.get());
        }
    }
}
