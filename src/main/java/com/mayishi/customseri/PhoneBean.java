package com.mayishi.customseri;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 序列化就是将内存中的对象,转换成字节序列(或其他传输协议),以便持久化到磁盘上存储和网络传输
 *
 * hadoop提供了序列化接口Writable,如果需要在Hadoop框架内传输一个对象,我们也需要将该对象序列化,则该自定义类必须要实现Writable接口
 * @author tty
 * @version 1.0 2021-03-31 09:30
 */
public class PhoneBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;
    private long avg;
    private long num;

    // 1. 反序列化时，需要反射调用空参构造函数，所以必须有
    public PhoneBean() {
        super();
    }

    public PhoneBean(long upFlow, long downFlow,long num) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
        this.num = num;
        this.avg = sumFlow /num;
    }

    //2. 写序列化方法
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //3. 反序列化方法
    //反序列化方法读顺序必须和写序列化方法的写顺序必须一致
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    //编写toString方法，方便后续打印到文本


    @Override
    public String toString() {
        return upFlow+ "\t" + downFlow + "\t" + sumFlow +"\t" + avg+"\t"+num;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public long getNum() {
        return num;
    }

    public void setNum(long num) {
        this.num = num;
    }

    public long getAvg() {
        return avg;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }
}
