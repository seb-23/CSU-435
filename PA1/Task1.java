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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import java.util.*; 

import com.google.common.collect.HashMultiset; 
import com.google.common.collect.Multiset; 

public class Task1 {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text node ;
		private Text n;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());  
			
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				node = new Text( s );
				String r = itr.nextToken();
				n = new Text( r );
				context.write(node, n); 
			}
		}
	}
	
	public static class CountReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private int count;
	
		@Override
		protected void setup(Context context) {
			count = 0;
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{ 
			for (Text value : values) {
				count++;
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text( Integer.toString(count)), NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// Create Hadoop Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "distinct nodes new");
		
		job.setJarByClass(Task1.class);
		job.setMapperClass(Task1.TokenizerMapper.class);
		job.setReducerClass(Task1.CountReducer.class);
		
		// There Is Only On Reducer
		job.setNumReduceTasks(1);
		
		// Output for Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// Input and Outpur Files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Did The Job Complete?
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
