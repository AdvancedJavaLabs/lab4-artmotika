package org.example.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SalesReducer extends Reducer<Text, SalesInfo, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<SalesInfo> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0.0;
        int totalQuantity = 0;

        for (SalesInfo value : values) {
            totalRevenue += value.getRevenue();
            totalQuantity += value.getQuantity();
        }

        context.write(key, new Text(String.format("%.2f\t%d", totalRevenue, totalQuantity)));
    }
}
