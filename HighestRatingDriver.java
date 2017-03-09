package A3.A3;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HighestRatingDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new HighestRatingDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception { //Args[0] = Input File, Args[1] = Output File, Args[2] = Movie ID File
		if (args.length != 2) {
			System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(HighestRatingDriver.class);
		job.setJobName("HighestRating");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(HighestRatingMapClass.class);
		job.setReducerClass(HighestRatingReduceClass.class);

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
        printTopTen(args, topTen);
	}
	public void printTopTen(String[] args, ArrayList<Item> topTen) throws IOException {
		File read = new File(args[1]);
        Scanner readIn = new Scanner(read);
		String[] titles = new String[10];
        
        for(int i = 0; i < 10; i++){
        	Stream<String> lines = Files.lines(Paths.get(args[2]));
        	titles[i] = lines.skip(Integer.parseInt(topTen.get(i).getID())).findFirst().get();
        }
        for(int i = 0; i < 10; i++){
        	System.out.println(titles[i]);
        }
	}
}