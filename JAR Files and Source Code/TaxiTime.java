import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiTime extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Job myJob = Job.getInstance(getConf(), "TaxiTime");
		myJob.setJarByClass(getClass());

		//Configuring the inputs and outputs
		TextInputFormat.addInputPath(myJob, new Path(args[0]));
		myJob.setInputFormatClass(TextInputFormat.class);
		TextOutputFormat.setOutputPath(myJob, new Path(args[1]));

		myJob.setMapOutputKeyClass(Text.class);
		myJob.setMapOutputValueClass(Text.class);
		myJob.setOutputFormatClass(TextOutputFormat.class);
		myJob.setOutputKeyClass(Text.class);
		myJob.setOutputValueClass(Text.class);

		// Configuring the Mapper and Reducer Classes
		myJob.setMapperClass(TaxiTimeMapper.class);
		myJob.setReducerClass(TaxiTimeReducer.class);

		myJob.setOutputKeyClass(Text.class);
		myJob.setOutputValueClass(Text.class);

		return myJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitStatus = ToolRunner.run(new TaxiTime(), args);
		System.exit(exitStatus);
	}
}

class TaxiTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String lines = value.toString();
		String[] values = lines.split(",");

		if(tryParse(values[20])){
			context.write(new Text(values[16]), new Text(values[20]));
		}
		if(tryParse(values[19])){
			context.write(new Text(values[17]), new Text(values[19]));
		}
	}
	boolean tryParse(String text){
		try{
			int x = Integer.parseInt(text);
			return true;
		}
		catch(NumberFormatException ex){
			return false;
		}
	}
}


class TaxiTimeReducer extends Reducer<Text, Text, Text, Text> {
	TreeSet<Pair> myList = new TreeSet<Pair>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		int taxiTime = 0;
		for(Text val: values){
			count+=1;
			taxiTime+=Integer.parseInt(val.toString());	
		}
		double avg = (double)taxiTime / (double) count;
		myList.add(new Pair(key.toString(),avg));
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("The Airports with the highest taxi time are:"), null);
		for(int i=0;i<3;i++){
			Pair last = myList.pollLast();
			context.write(new Text(last.airportName), new Text(Double.toString(last.taxiTime)));
		}

		context.write(new Text("The Airports with the lowest taxi time are:"), null);
		for(int i=0;i<3;i++){
			Pair first = myList.pollFirst();
			context.write(new Text(first.airportName), new Text(Double.toString(first.taxiTime)));
		}
	}

	class Pair implements Comparable<Pair>{
		String airportName;
		double taxiTime;

		public Pair(String flightName, double probablity) {
			this.airportName = flightName;
			this.taxiTime = probablity;
		}

		@Override
		public int compareTo(Pair pair) {
			if (this.taxiTime > pair.taxiTime) 
				return 1;
			else if(this.taxiTime < pair.taxiTime)
				return -1;
			else
				return 0;
		}
	}
}