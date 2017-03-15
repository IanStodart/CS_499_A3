package A3.p2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostReviewsMapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text userID = new Text();
	private static final IntWritable one = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, ",");

		while(st.hasMoreTokens()) {
			st.nextToken();
			userID.set(st.nextToken());
			context.write(userID, one);
		}
	}
}