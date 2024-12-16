package com.z01.StuSieve;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;

public class SieveReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            if(key.get() == 0) continue;
            context.write(NullWritable.get(), val);
        }
    }
}
