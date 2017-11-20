import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable();
    private int tweetLen;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    	  String[] fields = value.toString().split(";");

        if (fields.length == 4){
	        tweetLen = fields[2].length();
          if ((tweetLen >= 1) && (tweetLen <= 140)){
            double val = (double) tweetLen;

        data.set((int) Math.ceil(tweetLen/5));
        context.write(data, one);

          }

       }

    }
}
