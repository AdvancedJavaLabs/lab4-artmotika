package org.example.calculate;

import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;

public class SalesInfo implements Writable {
    private double revenue;
    private int quantity;

    public SalesInfo() {
    }

    public SalesInfo(double revenue, int quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeInt(quantity);
    }

    @Override
    public void readFields(java.io.DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readInt();
    }

    public double getRevenue() {
        return revenue;
    }

    public int getQuantity() {
        return quantity;
    }
}