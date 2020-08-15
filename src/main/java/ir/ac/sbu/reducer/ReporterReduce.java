package ir.ac.sbu.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReporterReduce extends Reducer<Text, Text, Text , Text> {
    @Override
    public void reduce(Text ranks, Iterable<Text> titles, Context context) throws IOException, InterruptedException {

        for (Text title : titles) {
            context.write(ranks,title);
        }

    }
}
