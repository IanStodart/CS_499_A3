

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestRatingReduceClass extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(Text text, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {

		int sum = 0;
		double ratingSum = 0;
		Iterator<DoubleWritable> i = values.iterator();
		while(i.hasNext()) {
			double count = i.next().get(); //current rating
			sum += 1;
			ratingSum += count;
		}

		context.write(text, new DoubleWritable(ratingSum/sum));
	}

}