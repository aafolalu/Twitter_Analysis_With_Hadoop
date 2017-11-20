import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class countReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values) {
            count += value.get();

        }

        result.set(count);
        context.write(key, result);

    }

}
