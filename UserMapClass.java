

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighestRatingMapClass extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text movieID = new Text();
	private DoubleWritable rating = new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, ",");

		while(st.hasMoreTokens()) {
			movieID.set(st.nextToken());
			st.nextToken();
			rating.set(st.nextToken().toCharArray()[0]);
			context.write(movieID, rating);
		}
	}

}