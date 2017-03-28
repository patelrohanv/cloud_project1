import java.io.File;
import java.util.Scanner;
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

public class Client{
    public static void main(String[] args) throws Exception{
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

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String token;

    public static class wordCountMapper extends Mapper<Object, Text, Text, IntWritable>{

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

    public static class wordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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

    public static boolean indexed(){
        File currDir = new File(".");
        currDir = new File(currDir.getAbsolutePath());
        File[] contents = currDir.listFiles();

        for (File f :contents) {
            if (f.toString().equals("WHATEVER WE NAME OUR INDEX")) {
                return true;
            }
        }
        return false;
    }

    public static void index()throws Exception{
        System.out.println("____________________________________________________________________");
        System.out.println("Creating index from directory.");
        System.out.println("____________________________________________________________________");

        Scanner kbd = new Scanner(System.in);
        System.out.println("Please enter the directory containing the files to be indexed:\t");
        String inResp = kbd.nextLine();
        Path inPath = new Path(inResp);
        System.out.println("Input directory located at:\t"+inPath.toString());
        System.out.println("Please enter the directory where the output should be saved:\t");
        String outResp = kbd.nextLine();
        Path outPath = new Path(outResp);
        System.out.println("Output directory located at:\t"+outPath.toString());

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Client.class);
        job.setMapperClass(wordCountMapper.class);
        job.setCombinerClass(wordCountReducer.class);
        job.setReducerClass(wordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println("____________________________________________________________________");
        return;
    }

    public static void search() {
        System.out.println("____________________________________________________________________");
        System.out.println("Searching...");
        System.out.println("____________________________________________________________________");

        //TODO Implement search

        System.out.println("____________________________________________________________________");
    }

    public static void indexFile() {
        System.out.println("____________________________________________________________________");
        System.out.println("Indexing file.");
        System.out.println("____________________________________________________________________");

        //TODO file indexing functionality

        System.out.println("____________________________________________________________________");
    }
}
