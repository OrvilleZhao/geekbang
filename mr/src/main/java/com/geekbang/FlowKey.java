package com.geekbang;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

public class FlowKey implements WritableComparable<FlowKey> {
    private Text iphone;

    public FlowKey(String str) {
        this.iphone = new Text(str);
    }

    @Override
    public int compareTo(FlowKey o) {
        byte[] bytes1 = this.iphone.getBytes();
        byte[] bytes2 = o.iphone.getBytes();
        for (int i=0; i<bytes1.length&&i<bytes2.length; i++){
            if ((int)bytes1[i] - (int)bytes2[i] != 0){
                return (int)bytes1[i] - (int)bytes2[i];
            }
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.iphone.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.iphone.readFields(dataInput);
    }
}
