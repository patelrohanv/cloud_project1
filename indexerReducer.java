import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public static class indexerReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  private Text term;
  private /*SOME TYPE*/ docID;
  private Text prevTerm;
  private Map postingList;

  public void initialize {
    prevTerm = null;
    postingList = new HashMap();
  }

  public void reduce(/*SOME TUPLE*/ key, IntWritable tf, Context context ) throws IOException, InterruptedException {
    term = /*SOME TUPLE.left*/;
    docID = /*SOME TUPLE.right*/;
    if (!term.equals(prevTerm) && prevTerm != null) {
      context.write(Text term, Map postingList);
      postingList.
    }
  }

  public void close {
    context.write(Text term, Map postingList);
  }
}

/*
class REDUCER
  method INITIALIZE {
    tprev <-- 0
    P <-- new PostingList
  }
  method REDUCE(tuple <t, d>, tf [f]) {
    if (t ≠ tprev) and (tprev ≠ 0) then {
      EMIT( term t, posting P );
      P.RESET
    }
    P.ADD(d, f)
    tprev <-- t
  }
  method CLOSE {
    EMIT( term t, posting P )
  }
}
*/
