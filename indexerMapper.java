import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.math.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class indexerMapper extends Mapper<IntWritable, Text, Text, IntWritable>{
    private IntWritable one = new IntWritable(1);
    private Text term = new Text();
    
    public void map(IntWritable docID, Text value, Context context) throws IOException, InterruptedException {
        HashMap<Text, IntWritable> H = new HashMap<Text, IntWritable>();
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            term.set(itr.nextToken());
            if (!H.containsKey(term)) {
                H.put(term, one);
            }
            else {
                IntWritable hTerm = H.get(term);
                int h = hTerm.get();
                h += 1;
                H.put(term, new IntWritable(h));
            }
        }

        for (Map.Entry<Text, IntWritable> entry : H.entrySet()) {
            term = entry.getKey();
            IntWritable freq = entry.getValue();
            IntWritable tf = new IntWritable(1 + (int)Math.log(freq.get()));
            indexPair key = new indexPair(term, docID);
            context.write(key.getKey(), tf);
        }
    }
    //INNER CLASS INDEXPAIR
    public class indexPair{
        public Text t;
        public IntWritable l;

        public indexPair(Text t, IntWritable l){
            this.t = t;
            this.l = l;
        }

        public Text getKey(){
            return t;
        }

        public IntWritable getValue(){
            return l;
        }


    }
}
