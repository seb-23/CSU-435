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
import java.io.File;

import java.util.*; 


public class Analysis {
	
	
	
	// Job #1
	
	public static class GeodesicAvgMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	
		private IntWritable n;
		private IntWritable m;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			n = new IntWritable( Integer.parseInt(itr.nextToken()) );
			m = new IntWritable( Integer.parseInt(itr.nextToken()) );
			
			context.write(n, m);
		}
	}
	
	public static class TotalNodesMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable( Integer.parseInt(value.toString()) ), new IntWritable(-1));
		}
	}
		
		
	public static class GeodesicAvgReducer extends Reducer<IntWritable, IntWritable, Text, NullWritable> {
		
		private long[] listOfSums;
		private long sum;
		private long vertices;
		private float geoavg;
		
		@Override
		protected void setup(Context context) {
			listOfSums = new long[11];
			sum = 0;
			vertices = 0;
			geoavg = 0.0f;
		}
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			long I = values.iterator().next().get();
			
			if (I == -1) {
				vertices = key.get();
			}
			else {
				sum += key.get() * I;
				listOfSums[key.get()] = I;
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			geoavg = (1.0f/(vertices*(vertices-1))) * sum;
			
			String print = "\n----------------------------------------------------------------------------------\n";
			for (int i = 0; i < 10; i++) {
				print += "| Path Length of " + Integer.toString(i+1) + " with " + Long.toString(listOfSums[i+1]) + " paths\n";
			}
			print += "----------------------------------------------------------------------------------\n";
			print += "| Total Shortest Path Length: " + Long.toString(sum) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			print += "| Total Number of Vertices: " + Long.toString(vertices) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			print += "| The Average Geodesic Path Length: " + Float.toString(geoavg) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			
			context.write(new Text(print), NullWritable.get());
		}
	}
	
	
	// Job #2
	
	public static class TotalTrianglesMapper extends Mapper<Object, Text, Text, Text> {
	
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text( value ), new Text("G"));
		}
	}
	
	public static class TotalTripletsMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new Text( value ), new Text("P"));
		}
	}
		
		
	public static class ClusterCoefficientReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		float triangles;
		long triplets;
		
		@Override
		protected void setup(Context context) {
			triangles = 0;
			triplets = 0;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String S = values.iterator().next().toString();
			
			if (S.charAt(0) == 'G') {
				triangles = Float.parseFloat(key.toString());
			}
			else {
				triplets = Integer.parseInt(key.toString());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			float coefficient = (3.0f * triangles)/triplets;
			
			String print = "\n----------------------------------------------------------------------------------\n";
			print += "| Total Number of Triples: " + Long.toString(triplets) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			print += "| Total Number of Triangles: " + Float.toString(triangles) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			print += "| Global Clustering Coefficient: " + Float.toString(coefficient) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			
			context.write(new Text(print), NullWritable.get());
		}
	}
	
	
	// Job #3
	
	public static class UniqueEdgesMapper extends Mapper<Object, Text, IntWritable, Text> {
	
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable( Integer.parseInt(value.toString()) ), new Text("E"));
		}
	}
	
	public static class UniqueNodesMapper extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			context.write(new IntWritable( Integer.parseInt(value.toString()) ), new Text("N"));
		}
	}
		
		
	public static class RandomReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		
		float edges;
		float nodes;
		
		@Override
		protected void setup(Context context) {
			edges = 0;
			nodes = 0;
		}
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String S = values.iterator().next().toString();
			
			if (S.charAt(0) == 'E') {
				edges = key.get();
			}
			else {
				nodes = key.get();
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			float k = edges/nodes;
			float Lrandom = (float)Math.log(nodes)/(float)Math.log(k);
			float Crandom = k/nodes;
			
			String print = "\n----------------------------------------------------------------------------------\n";
			print += "| L-random: " + Float.toString(Lrandom) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			print += "| C-random: " + Float.toString(Crandom) + "\n";
			print += "----------------------------------------------------------------------------------\n";
			
			context.write(new Text(print), NullWritable.get());
		}
	}
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		// Job #1
		Configuration conf5 = new Configuration();
		Job job5 = Job.getInstance(conf5, "Geodesic Average");
		
		job5.setJarByClass(Analysis.class);
		job5.setReducerClass(Analysis.GeodesicAvgReducer.class);
		
		job5.setNumReduceTasks(1);
		
		job5.setMapOutputKeyClass(IntWritable.class);
		job5.setMapOutputValueClass(IntWritable.class);
		
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(job5, new Path(args[0]), TextInputFormat.class, Analysis.GeodesicAvgMapper.class);
		MultipleInputs.addInputPath(job5, new Path(args[1]), TextInputFormat.class, Analysis.TotalNodesMapper.class);
		
		FileOutputFormat.setOutputPath(job5, new Path(args[2]));
		
		job5.waitForCompletion(true);
		
		
		// Job #2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Cluster Coefficient");
		
		job2.setJarByClass(Analysis.class);
		job2.setReducerClass(Analysis.ClusterCoefficientReducer.class);
		
		job2.setNumReduceTasks(1);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, Analysis.TotalTrianglesMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, Analysis.TotalTripletsMapper.class);
		
		FileOutputFormat.setOutputPath(job2, new Path(args[5]));
		
		job2.waitForCompletion(true);
		
		
		// Job #3
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "C-random & L-random");
		
		job3.setJarByClass(Analysis.class);
		job3.setReducerClass(Analysis.RandomReducer.class);
		
		job3.setNumReduceTasks(1);
		
		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(NullWritable.class);
		
		MultipleInputs.addInputPath(job3, new Path(args[6]), TextInputFormat.class, Analysis.UniqueEdgesMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class, Analysis.UniqueNodesMapper.class);
		
		FileOutputFormat.setOutputPath(job3, new Path(args[7]));
		
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		

	}
}






