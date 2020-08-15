package ir.ac.sbu.mapper;

import ir.ac.sbu.types.Node;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Text, Node, DoubleWritable, Text> {
    @Override
    protected void map(Text key, Node value, Context context) throws IOException, InterruptedException {
//        String ss = value.toString();
//        context.write(key,new Text(ss));
        double pr = value.getRank();
        context.write(new DoubleWritable(-1*pr),key);
    }
}
