import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class indexerMapper extends Mapper<Object, Text, Text, IntWritable>{

  private IntWritable one = new IntWritable(1);
  private LongWritable id = new LongWritable();
  private Text term = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Map H = new HashMap();
    StringTokenizer itr = new StringTokenizer(value.toString());

    while (itr.hasMoreTokens()) {
      term.set(itr.nextToken());
      if (!H.containsKey(term))
      H.put(term, one);
      else
      H.put(term, H.get(term) + 1);
    }

    for (Map.Entry<Text, IntWritable> entry : H.entrySet()) {
      term = entry.getKey();
      IntWritable freq = entry.getValue();
      IntWritable tf = 1 + Math.log(freq);
      Pair<Text, LongWritable> key = new Pair<Text, LongWritable>(id, term);
      context.write(key, tf);
    }
  }
}
