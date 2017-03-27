import java.io.IOException;
import java.util.StringTokenizer;

//import org.apache.opennlp.tools.stemmer.PorterStemmer;
//import org.apache.lucene.analysis.PorterStemmer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String token;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                token = preProcess(itr.nextToken());
                //token = itr.nextToken();
                String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
                token = token+"^"+fileName;
                word.set(token);
                context.write(word, one);
            }
        }
        public String preProcess(String s) {
            String stemmed, lower, alpha;                       //
            //PorterStemmer stemmer = new PorterStemmer();        //
            //stemmed = stemmer.stem(s);                          //perform stemming
            //lower = stemmed.toLowerCase();                      //convert to lowercase
            lower = s.toLowerCase();                            //convert to lowercase
            alpha = lower.replaceAll("[^a-zA-Z]", "");          //retain only alphabet chars
            return alpha;
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SecondMapper extends Mapper<Text, IntWritable, Text, IntWritable>{}

    public static class SecondReducer extends Reducer<Text,IntWritable,Text,IntWritable> {}

    public static void main(String[] args) throws Exception {
        System.out.println("____________________________________________________________________");
        System.out.println("Welcome to tiny-Google");
        System.out.println("____________________________________________________________________");
        int input;
        Scanner kbd = new Scanner(System.in);
        if (!indexed()) {
            while(true) {
                System.out.println("No index found on disk.\nWould you like to generate one now?\n\t1. Yes\n\t2. No");
                input = kbd.nextInt();
                if(input > 2 || input < 1){
                    System.out.println("Not a valid option. Please try again.\n");
                }
                else if (input == 1) {
                    index();
                    break;
                }
                else {
                    System.out.println("Ok. Note: No searches will be possible until an index is generated.");
                    System.out.println("____________________________________________________________________");
                    break;
                }
            }
        }

        do{
            System.out.println("Enter an option:\n\t1. Search for a word \n\t2. Add a document \n\t3. Generate Index from Directory\n\t4. Quit");
            input = kbd.nextInt();
            if(input > 4 || input < 1){
                System.out.println("Not a valid option.\nPlease try again.");
                continue;
            }
            if (input==1 && !indexed()) {
                System.out.println("Search not possible untill index is generated.");
            }
            else if (input == 1) {
                search();
            }
            else if (input == 2) {
                indexFile();
            }
            else if (input == 3) {
                index();
            }
        }while(input != 4);
        System.out.println("____________________________________________________________________");
        System.out.println("Goodbye!");
        System.out.println("____________________________________________________________________");
    }

    public static void mapreduce(Strin[] args){
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
