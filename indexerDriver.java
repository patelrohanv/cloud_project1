import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.*;
import java.lang.InterruptedException;
import java.lang.ClassNotFoundException;
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


public class indexerDriver{
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        indexer(new Path(args[0]), new Path(args[1]));
    }
    public static void indexer(Path input, Path output) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(indexerDriver.class);
        job.setMapperClass(indexerMapper.class);
        job.setCombinerClass(indexerReducer.class);
        job.setReducerClass(indexerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


//REDUCER
class indexerReducer extends Reducer<Text,IntWritable,Text,HashMap<IntWritable, IntWritable>> {
    private Text term = new Text();
    private Text prevTerm = new Text();
    private IntWritable docID = new IntWritable();
    private HashMap<IntWritable, IntWritable> postingList;

    public void initialize() {
        prevTerm = null;
        postingList = new HashMap<IntWritable, IntWritable>();
    }

    public void reduce(indexPair key, IntWritable tf, Context context ) throws IOException, InterruptedException {
        term = key.getKey();
        docID = key.getValue();
        if (!term.equals(prevTerm) && prevTerm != null) {
            context.write(term, postingList);
            postingList = new HashMap();
        }
        postingList.put(docID, tf);
        prevTerm = term;
    }

    public void close(Context context) throws IOException, InterruptedException{
        context.write(term, postingList);
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

//MAPPER
class indexerMapper extends Mapper<IntWritable, Text, Text, IntWritable>{
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
