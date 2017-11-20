import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int len = 0;

        for (IntWritable value : values) {

            len += value.get();

        }

        result.set(len);
        context.write(key, result);

    }

}
