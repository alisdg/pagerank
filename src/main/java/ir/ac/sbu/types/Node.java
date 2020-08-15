package ir.ac.sbu.types;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Node implements WritableComparable<Node> {
    private List<String> neighbours;
    private double rank ;
    private boolean isProcessing ;

    public Node(boolean isProcessing) {
        this.neighbours = new ArrayList<>();
        this.isProcessing = isProcessing;
    }

    public Node() {
        this.neighbours = new ArrayList<>();
        this.isProcessing = false ;
    }

    public List<String> getNeighbours() {
        return neighbours;
    }

    public void setNeighbours(List<String> neighbours) {
        this.neighbours = neighbours;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public boolean isProcessing() {
        return isProcessing;
    }

    public void setProcessing(boolean processing) {
        isProcessing = processing;
    }

    public void addNeighbour(String child) {
        this.neighbours.add(child);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new DoubleWritable(rank).write(out);
        new BooleanWritable(isProcessing).write(out);
        new IntWritable(neighbours.size()).write(out);
        for (String child : neighbours) {
            new Text(child).write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        DoubleWritable doubleWritable = new DoubleWritable();
        doubleWritable.readFields(in);
        this.rank = doubleWritable.get();
        BooleanWritable isWritable = new BooleanWritable();
        isWritable.readFields(in);
        this.isProcessing = isWritable.get();
        IntWritable intWritable = new IntWritable();
        intWritable.readFields(in);
        int count = intWritable.get();
        setNeighbours(new ArrayList<>());
        for (int i = 0; i < count; i++) {
            Text link = new Text();
            link.readFields(in);
            neighbours.add(link.toString());
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("");
        for (String n : neighbours) {
            stringBuilder
                    .append(n)
                    .append(" ");
        }
        stringBuilder.append(rank);
        stringBuilder.append("\n");
        return stringBuilder.toString();
    }

    @Override
    public int compareTo(Node o) {
        double diff = (this.getRank() - o.getRank());
        return diff > 0 ? 1 : (diff == 0 ? 0 : -1) ;
    }

    public boolean hasLink(){
        return !this.neighbours.isEmpty();
    }
    public int linksCount(){
        return neighbours.size();
    }

    public String getLink(int i){
        return this.neighbours.get(i);
    }
}