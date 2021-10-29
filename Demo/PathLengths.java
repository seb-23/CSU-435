import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.google.common.collect.Multisets;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import java.util.*; 


public class PathLengths {
	
	
	
	
	
	public static class PathsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] s = value.toString().split("~",0);
			int v = s.length-1;
			context.write(new IntWritable(v), new IntWritable(1));
				
			
		}
	}
	
	public static class PathCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable I : values) {
				count++;
			}

			context.write(key, new IntWritable(count));
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		
		// Job #1
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Length Path Counts");
		
		job1.setJarByClass(PathLengths.class);
		//job1.setMapperClass(PathLengths.PathsMapper.class);
		job1.setReducerClass(PathLengths.PathCountReducer.class);
		
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[3]), TextInputFormat.class, PathLengths.PathsMapper.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[4]), TextInputFormat.class, PathLengths.PathsMapper.class);
		
		MultipleInputs.addInputPath(job1, new Path(args[5]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[6]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[7]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[8]), TextInputFormat.class, PathLengths.PathsMapper.class);
		MultipleInputs.addInputPath(job1, new Path(args[9]), TextInputFormat.class, PathLengths.PathsMapper.class);
		
		
		//FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[10]));
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
	}
}






