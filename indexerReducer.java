import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.tuple.Pair<L,R>;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public static class indexerReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  private Text term = new Text();
  private Text prevTerm = new Text();
  private LongWritable docID = new LongWritable();
  private HashMap<Text, LongWritable> postingList;

  public void initialize() {
    prevTerm = null;
    postingList = new HashMap<Text, LongWritable>();
  }

  public void reduce(Pair<Text, LongWritable> key, IntWritable tf, Context context ) throws IOException, InterruptedException {
    term = key.getKey();
    docID = key.getValue();
    if (!term.equals(prevTerm) && prevTerm != null) {
      context.write(term, postingList);
      postingList = new HashMap();
    }
    postingList.put(docID, tf);
    prevTerm = term;
  }

  public void close() {
    context.write(term, postingList);
  }
}
