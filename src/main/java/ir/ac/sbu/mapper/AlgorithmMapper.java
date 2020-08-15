package ir.ac.sbu.mapper;

import ir.ac.sbu.types.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AlgorithmMapper extends Mapper<Text, Node, Text, Node> {
    @Override
    protected void map(Text key, Node value, Context context) throws IOException, InterruptedException {
        double weightage ;
        if (value.hasLink()) {
            int counts = value.linksCount();
            weightage = value.getRank() / counts;
            for (int i = 0; i < counts; i++) {
                Node n = new Node(false);
                n.setRank(-1*weightage);
                context.write(new Text(value.getLink(i)), n);
            }
        }
        context.write(key, value);
    }
}
