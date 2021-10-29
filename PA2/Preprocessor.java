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

import com.google.common.collect.Multisets;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import java.util.*; 


public class Preprocessor {
	
	public static class NodesMapper extends Mapper<Object, Text, Text, NullWritable> {
		
		private Text node ;
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				
				node = new Text( itr.nextToken() );
				context.write(node, NullWritable.get());
				
			}
		}
	}
	
	public static class CountNodesReducer extends Reducer<Text, NullWritable, IntWritable, NullWritable>{
		private Set<String> distinctNodes ;
		@Override
		protected void setup(Context context) {
			distinctNodes = new HashSet<String>();
		}
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
			distinctNodes.add(key.toString());
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			IntWritable noOfNodes = new IntWritable(distinctNodes.size());
			context.write(noOfNodes, NullWritable.get());
		}
	}
	
	
	
	public static class EdgesMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text n;
		private Text m;
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				
				n = new Text( itr.nextToken() );
				m = new Text( itr.nextToken() );
				context.write(n, m);
			}
		}
	}
	
	public static class CountEdgesReducer extends Reducer<Text, Text, IntWritable, NullWritable>{
		
		private Set<String> distinctEdges ;
		
		@Override
		protected void setup(Context context) {
			distinctEdges = new HashSet<String>();
			
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) {
			for (Text val : values) {
				String v = val.toString();
				String s = key.toString();
				
				if (s.length() > v.length() || (s.length() == v.length() && s.compareTo(v) > 0) ) {
					distinctEdges.add(v + "~" + s);
				}
				
				else {
					distinctEdges.add(s + "~" + v);
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new IntWritable( distinctEdges.size() ), NullWritable.get());
			
		}
	}
	
	
	
	
	
	
	public static class RemoveEdgesReducer extends Reducer<Text, Text, Text, Text>{
		
		private Set<String> distinctEdges ;
		
		@Override
		protected void setup(Context context) {
			distinctEdges = new HashSet<String>();
			
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) {
			for (Text val : values) {
				String v = val.toString();
				String s = key.toString();
				
				if (s.length() > v.length() || (s.length() == v.length() && s.compareTo(v) > 0) ) {
					distinctEdges.add(v + "~" + s);
				}
				
				else {
					distinctEdges.add(s + "~" + v);
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {	
			for (String s : distinctEdges) {
				String[] str = s.split("~",2);
				context.write(new Text(str[0]), new Text(str[1]));
			}
		}
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		// Job #1
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Total Number of Vertices:");
		
		job1.setJarByClass(Preprocessor.class);
		job1.setMapperClass(Preprocessor.NodesMapper.class);
		job1.setReducerClass(Preprocessor.CountNodesReducer.class);
		
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(NullWritable.class);
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.waitForCompletion(true);
		
		
		
		// Job #2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Total Number of Edges:");
		
		job2.setJarByClass(Preprocessor.class);
		job2.setMapperClass(Preprocessor.EdgesMapper.class);
		job2.setReducerClass(Preprocessor.CountEdgesReducer.class);
		
		job2.setNumReduceTasks(1);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		job2.waitForCompletion(true);
		
		
		// Job #3
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "Remove Duplicate Edges:");
		
		job3.setJarByClass(Preprocessor.class);
		job3.setMapperClass(Preprocessor.EdgesMapper.class);
		job3.setReducerClass(Preprocessor.RemoveEdgesReducer.class);
		
		job3.setNumReduceTasks(1);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		
	}
}






