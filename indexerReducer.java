import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class indexerReducer extends Reducer<Text,IntWritable,Text,HashMap<IntWritable, IntWritable>> {
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
