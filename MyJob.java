package cps534_2.pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MyJob extends Configured implements Tool {
	
	private static final long desiredConvergence = 20;


	public int run(String[] args) throws Exception {
		
		while (true) {
			String jobName = "pageRank";
			Job job = Job.getInstance(new Configuration(), jobName);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
	    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.setJarByClass(MyJob.class);
			
			job.submit();
			
			// Counter
			long summedConvergence = job.getCounters().findCounter(MyReducer.Counter.DELTAS).getValue();
			if (summedConvergence < desiredConvergence) { // designedConvergence is fixed
				break; // done with all iterations
			}
			return 0;
		}
		return -1;
	}
	
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MyJob(), args);
		if(args.length != 2) {
			System.err.println("Usage: <in> <output name>");
			System.exit(res);
		}
	}
}