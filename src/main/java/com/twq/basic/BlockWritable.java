package com.twq.basic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BlockWritable implements Writable {
    private long blockId;
    private long numBytes;
    private long generationStamp;

    public BlockWritable() {
    }

    public BlockWritable(long blockId) {
        this.blockId = blockId;
    }

    public BlockWritable(long blockId, long numBytes, long generationStamp) {
        this.blockId = blockId;
        this.numBytes = numBytes;
        this.generationStamp = generationStamp;
    }

    public long getBlockId() {
        return blockId;
    }

    public long getNumBytes() {
        return numBytes;
    }

    public long getGenerationStamp() {
        return generationStamp;
    }

    @Override
    public String toString() {
        return "Block{" +
                "blockId=" + blockId +
                ", numBytes=" + numBytes +
                ", generationStamp=" + generationStamp +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(blockId);
        dataOutput.writeLong(numBytes);
        dataOutput.writeLong(generationStamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.blockId = dataInput.readLong();
        this.numBytes = dataInput.readLong();
        this.generationStamp = dataInput.readLong();
    }
}
