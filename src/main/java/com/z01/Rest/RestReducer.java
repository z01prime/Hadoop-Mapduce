package com.z01.Rest;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RestReducer extends Reducer<Rest, Text, Text, NullWritable> {
    @Override
    protected void reduce(Rest key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 对每个 key 输出一次，因为 sort 阶段已经按照顺序排好了
        for (Text value : values) {
//            context.write(new Text(value.toString() + "," + key.toString()), NullWritable.get());
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }
}