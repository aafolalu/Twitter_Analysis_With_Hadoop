import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Hashtable<String, String> companyInfo;

	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();
	private int hours;
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


	try{
				 String[] fields = value.toString().split(";");

				 if (fields.length == 4){

					 for (String athlete : companyInfo.keySet()){
					 				//tweet containing athlete names
					 				if (fields[2].contains(athlete)) {
										data.set(athlete);
										context.write(data, one);
					 				}
							}
					}

					}

		catch(Exception e) {

		}

			}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		companyInfo = new Hashtable<String, String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				String[] fields = line.split(",");
				// Fields are: 0:id 1:name 2:nationality 3:sex 4:dob 5:height 6:weight 7:sport 8:gold 9:silver 10:bronze
				if (fields.length == 11)
					companyInfo.put(fields[7], fields[0]);
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
