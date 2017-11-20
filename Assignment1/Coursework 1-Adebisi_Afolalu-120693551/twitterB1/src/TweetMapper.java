import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TweetMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private IntWritable data = new IntWritable();
    private int hours;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    try{
    	     String[] fields = value.toString().split(";");

           if (fields.length == 4){

                long epochTime =  Long.parseLong(fields[0]);
                LocalDateTime dateTime = LocalDateTime.ofEpochSecond(epochTime/1000, 0, ZoneOffset.UTC);
                hours = dateTime.getHour();

                data.set(hours);
                context.write(data, one);

            }

      }

      catch(Exception e) {

      }

    }
 }
