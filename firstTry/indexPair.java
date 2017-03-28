import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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