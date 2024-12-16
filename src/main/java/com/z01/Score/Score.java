package com.z01.Score;

import org.apache.hadoop.io.*;
import java.io.*;

public class Score implements WritableComparable<Score> {
    private int score;
    private int age;

    // 默认构造函数
    public Score() {}

    // 构造函数
    public Score(int score, int age) {
        this.score = score;
        this.age = age;
    }

    // 读入数据
    @Override
    public void readFields(DataInput in) throws IOException {
        score = in.readInt();
        age = in.readInt();
    }

    // 写出数据
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(score);
        out.writeInt(age);
    }

    // compareTo 方法：首先按 Score 排序，如果相等再按 Age 排序
    @Override
    public int compareTo(Score o) {
        // 按 Score 排序（降序）
        int scoreComparison = Integer.compare(o.score, this.score);
        if (scoreComparison != 0) {
            return scoreComparison;
        }
        // 如果 Score 相等，按 Age 排序（降序）
        return Integer.compare(o.age, this.age);
    }

    // toString 方法，方便调试输出
    @Override
    public String toString() {
        return score + "," + age;
    }
}