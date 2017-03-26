import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class indexerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private LongWritable one = new LongWritable(1);
    private Text term = new Text();
    
    public void map(LongWritable docID, Text value, Context context) throws IOException, InterruptedException {
        HashMap<Text, LongWritable> H = new HashMap<Text, LongWritable>();
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            term.set(itr.nextToken());
            if (!H.containsKey(term)) {
                H.put(term, one);
            }
            else {
                H.put(term, H.get(term) + 1);
            }
        }

        for (Map.Entry<Text, LongWritable> entry : H.entrySet()) {
            term = entry.getKey();
            LongWritable freq = entry.getValue();
            LongWritable tf = 1 + Math.log(freq);
            Pair<Text, LongWritable> key = new Pair<Text, LongWritable>(docID, term);
            context.write(key, tf);
        }
    }
}
