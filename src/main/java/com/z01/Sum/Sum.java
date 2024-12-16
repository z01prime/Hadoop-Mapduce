package com.z01.Sum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sum implements WritableComparable<Sum> {
    private int sum;  // Sum 用来排序

    // 默认构造函数
    public Sum() {}

    // 构造函数
    public Sum(int sum) {
        this.sum = sum;
    }

    // 读入数据
    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readInt();
    }

    // 写出数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(sum);
    }

    // compareTo 方法：按 Sum 排序（降序）
    @Override
    public int compareTo(Sum o) {
        // 按 Sum 排序（降序）
        return Integer.compare(o.sum, this.sum);
    }

    // toString 方法，方便调试输出
    @Override
    public String toString() {
        return String.valueOf(sum);
    }
}