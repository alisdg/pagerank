package ir.ac.sbu.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<Text, Text, Text , Text> {
    @Override
    public void reduce(Text ranks, Iterable<Text> titles, Context context) throws IOException, InterruptedException {
//        double temp = 0.0;
//        String t = "";
//        temp = ranks.get() * -1;
//        for (Text title : titles) {
//            t = title.toString();
//            context.write(new Text(t), new DoubleWritable(temp));
//        }
        for (Text title : titles) {
            context.write(ranks,title);
        }

    }
}
