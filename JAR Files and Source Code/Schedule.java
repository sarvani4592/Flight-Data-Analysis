import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Schedule extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Job myJob = Job.getInstance(getConf(), "Schedule");
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
		myJob.setMapperClass(ScheduleMapper.class);
		myJob.setReducerClass(ScheduleReducer.class);

		myJob.setOutputKeyClass(Text.class);
		myJob.setOutputValueClass(Text.class);

		return myJob.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		int exitStatus=ToolRunner.run(new Schedule(), args);
		System.exit(exitStatus);
	}
}
class ScheduleMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String lines = value.toString();
		String[] values = lines.split(",");
		if(tryParse(values[14])>10)
			context.write(new Text(values[8]), new Text("1"));
		else
			context.write(new Text(values[8]), new Text("0"));
	}

	int tryParse(String text){
		try{
			int x = Integer.parseInt(text);
			return x;
		}
		catch(NumberFormatException ex){
			return 0;
		}
	}
}

class ScheduleReducer extends Reducer<Text, Text, Text, Text> {
	TreeSet<Pair> myList = new TreeSet<Pair>();
	public void reduce(Text key, Iterable<Text> values, Context context) {
		int onTimeCount = 0;
		int lateCount = 0;

		for(Text val: values){
			if(Integer.parseInt(val.toString()) == 0)
				onTimeCount+=1;
			else if(Integer.parseInt(val.toString()) == 1)
				lateCount+=1;
		}

		double prob = (double)onTimeCount / (double)(onTimeCount+lateCount);
		myList.add(new Pair(key.toString(), prob));
	}


	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("The Flights with the highest probability of being late are:"), null);
		for (int i=0; i<3; i++) {
			Pair last = myList.pollLast();
			context.write(new Text(last.flightName), new Text(Double.toString(last.probablity)));
		}

		context.write(new Text("The Flights with the least probability of being late are:"), null);
		for(int i=0;i<3;i++){
			Pair first = myList.pollFirst();
			context.write(new Text(first.flightName), new Text(Double.toString(first.probablity)));
		}
	}

	class Pair implements Comparable<Pair>{
		String flightName;
		double probablity;


		public Pair(String flightName, double probablity) {
			this.flightName = flightName;
			this.probablity = probablity;
		}

		@Override
		public int compareTo(Pair p) {
			if (this.probablity > p.probablity) 
				return 1;
			else if(this.probablity<p.probablity)
				return -1;
			else
				return 0;
		}
	}	
}