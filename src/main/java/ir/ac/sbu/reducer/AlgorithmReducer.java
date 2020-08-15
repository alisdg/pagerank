package ir.ac.sbu.reducer;

import ir.ac.sbu.types.Node;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AlgorithmReducer extends Reducer<Text, Node, Text, Node> {
    @Override
    protected void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
        double dampingFactor = context.getConfiguration().getDouble("damp", 0.85);
        double contributions = 0.0;
        Node primary = new Node(true);
//
//        for (Node val : values) {
//            if(val.isProcessing()) {
//                primary = val ;
//                continue;
//            }
//            contributions += val.getRank();
//            context.write(key,val);
//        }
//
//        //Compute new page rank
//        double pageRank = (1.0 - dampingFactor) + (dampingFactor * contributions);
//        primary.setRank(pageRank)
//        context.write(key, primary);

        for (Node val : values) {
            if (val.isProcessing()) {
                primary = val ;
            }
        }
        context.write(key,primary);
    }
}
