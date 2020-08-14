package ir.ac.sbu.reducer;

import ir.ac.sbu.types.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PreReducer extends Reducer<Text, Text, Text, Node> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Node node = new Node(true);
        node.setRank(1);
        for (Text value : values) {
            node.addNeighbour(value.toString());
        }
        context.write(key, node);
    }

}
