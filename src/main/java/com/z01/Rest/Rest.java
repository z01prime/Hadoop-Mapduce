package com.z01.Rest;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Rest implements WritableComparable<Rest> {
    private int Rest;  // Rest 用来排序

    // 默认构造函数,反序列化时，需要反射调用空参构造函数，所以必须有空参构造
    public Rest() {}

    // 构造函数
    public Rest(int Rest) {
        this.Rest = Rest;
    }

    // 读入数据
    @Override
    public void readFields(DataInput in) throws IOException {
        Rest = in.readInt();
    }

    // 写出数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(Rest);
    }

    // compareTo 方法：按 Rest 排序（降序）
    @Override
    public int compareTo(Rest o) {
        // 按 Rest 排序（升序）
        return Integer.compare(this.Rest,o.Rest);
    }

    // 重写toString()方法，把结果显示在文件中，方便调试输出
    @Override
    public String toString() {
        return String.valueOf(Rest);
    }
}