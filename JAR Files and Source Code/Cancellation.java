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

public class Cancellation extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		Job myJob = Job.getInstance(getConf(), "Cancellation");
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
		myJob.setMapperClass(CancellationMapper.class);
		myJob.setReducerClass(CancellationReducer.class);

		myJob.setOutputKeyClass(Text.class);
		myJob.setOutputValueClass(Text.class);

		return myJob.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitStatus = ToolRunner.run(new Cancellation(), args);
		System.exit(exitStatus);
	}
}


class CancellationMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		int count = 0;
		String lines = value.toString();
		String[] values = lines.split(",");
		if(tryParse(values[21])==1){
			context.write(new Text(values[22]), new Text("1"));
		}
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

class CancellationReducer extends Reducer<Text, Text, Text, Text> {
	TreeSet<Pair> myList = new TreeSet<Pair>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(Text val: values){
			count += Integer.parseInt(val.toString());
		}
		myList.add(new Pair(key.toString(), count));
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		String reason = "";

		Pair pair = myList.pollLast();
		switch(pair.reason){
		case "A": reason = "Carrier Delay";
		break;
		case "B": reason = "Weather Delay";
		break;
		case "C": reason = "NAS Delay";
		break;
		case "D": reason = "Security Delay";
		break;
		}
		context.write(new Text("The most common reason of flight delay is"), new Text(reason));
	}


	class Pair implements Comparable<Pair>{
		String reason;
		int count;

		public Pair(String reason, int count) {
			this.reason = reason;
			this.count = count;
		}

		@Override
		public int compareTo(Pair pair) {
			if (this.count > pair.count) 
				return 1;
			else if (this.count < pair.count)
				return -1;
			else
				return 0;
		}
	}
}