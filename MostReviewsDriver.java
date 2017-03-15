package A3.p2;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MostReviewsDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MostReviewsDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception { //Args[0] = Input File, Args[1] = Output File, Args[2] = Movie ID File
		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(MostReviewsDriver.class);
		job.setJobName("MostReviews");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(MostReviewsMapClass.class);
		job.setReducerClass(MostReviewsReduceClass.class);

		int returnValue = job.waitForCompletion(true) ? 0:1;

		if (job.isSuccessful()) {
			System.out.println("Job was successful");
			topTen(args);
		} else if(!job.isSuccessful()) {
			System.out.println("Job was not successful");
		}

		return returnValue;
	}
	public void topTen(String[] args) throws IOException {
		File read = new File(args[1]);
        Scanner readIn = new Scanner(read);
        
        ArrayList<Item> data = new ArrayList<Item>();
        
        while(readIn.hasNext()){
        	StringTokenizer st = new StringTokenizer(readIn.nextLine(), " ");
        	data.add(new Item(st.nextToken(),Double.parseDouble(st.nextToken())));
        }
        readIn.close();
        
        Collections.sort(data);
        ArrayList<Item> topTen = new ArrayList<Item>();
        for(int i = 0; i < 10; i++){
        	topTen.add(data.get(i)); //Copy Top 10 Rated Movies to new Array
        }
        printTopTen(topTen);
	}
	public void printTopTen(ArrayList<Item> topTen)  {
        
        for(int i = 0; i < 10; i++){
        	System.out.println(topTen.get(i));
        }
	}
}