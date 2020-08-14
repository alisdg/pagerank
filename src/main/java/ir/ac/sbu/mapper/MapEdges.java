package ir.ac.sbu.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapEdges extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] valArr = value.toString().split(" ");
        String from = valArr[0];
        String to = valArr[1];
        context.write(new Text(from), new Text(to));
    }
}
