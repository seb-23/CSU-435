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

import com.google.common.collect.HashMultiset; 
import com.google.common.collect.Multiset;



public class Cluster {
		
	public static class GenerateTriplesMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text n;
		private Text m;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {

				n = new Text( itr.nextToken() );
				m = new Text( itr.nextToken() );
				
				context.write(n, m);
				context.write(m, n);
			}
		}
	}
	
	public static class GenerateTriplesReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		
		class Combination { 
		  
			public void combinationUtil(ArrayList<String> s, String data[], int start, int end, int index, ArrayList<String> out) { 
				if (index == 2) { 
					for (int j=0; j<2; j++) {
						out.add(data[j]);
					}
					return;
				} 
		  
				for (int i=start; i<=end && end-i+1 >= 2-index; i++) { 
					data[index] = s.get(i); 
					combinationUtil(s, data, i+1, end, index+1, out); 
				} 
			} 
		  
			public void Combinations(ArrayList<String> s, int n, ArrayList<String> out) { 
				String data[]=new String[2]; 
				combinationUtil(s, data, 0, n-1, 0, out); 
			}
		}
		
		private ArrayList<String> nodes;
		private Combination combo;
	
		@Override
		protected void setup(Context context) {
			combo = new Combination();
			nodes = new ArrayList<String>();
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			nodes.clear();
			for (Text value : values) {
				nodes.add(value.toString());
			}
			ArrayList<String> out = new ArrayList<String>();
			combo.Combinations(nodes, nodes.size(), out);
			for (int j = 0; j < out.size()/2; j++) {
				String s = out.get(2*j) + "~" + key.toString() + "~" + out.get(2*j+1);
				context.write(new Text(s), NullWritable.get());
			}
		}
	}
	
	// CountTriplesMapper Input:  <triple, null>
	// CountTriplesMapper Output:  <"A", 1>
	// CountTriplesReducer Input:  <"A", listof1s>
	// CountTriplesReducer Output: <count, null>
	
	public static class CountTriplesMapper extends Mapper<Object, Text, Text, IntWritable> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				itr.nextToken();
				context.write(new Text("A"), new IntWritable(1));
			}
		}
	}
	
	public static class CountTriplesReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
		
		private int count;
	
		@Override
		protected void setup(Context context) {
			count = 0;
		}
	
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			for (IntWritable value : values) {
				count++;
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException { 
			context.write(new IntWritable(count), NullWritable.get());
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static class GenerateTrianglesMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text n;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				n = new Text( itr.nextToken() );
				String s = n.toString();
				String[] str = s.split("~",3);
				context.write(new Text( str[0] + "~" + str[2]  ), n);
			}
		}
	}
	
	public static class CreateTrianglesMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {

				String s = itr.nextToken();
				String c = itr.nextToken();
				
				context.write(new Text(s+"~"+c), new Text("1"));
				context.write(new Text(c+"~"+s), new Text("1"));
			}
		}
	}
	
	
	public static class CreateTrianglesReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		ArrayList<String> arr = new ArrayList<String>();
		private float total;
		
		@Override
		protected void setup(Context context) {
			arr = new ArrayList<String>();
			total = 0;
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			arr.clear();
			int count = 0;
			for (Text val : values) {
				if (val.toString().contains("~")) {
					arr.add(val.toString());
				}
				if (val.toString().equals("1")) {
					count += 1;
				}
			}
			
			if (count > 0 && arr.size() > 0) {
				for (String s : arr) {
					//context.write(new Text(s), NullWritable.get());
					total++;
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			float i = total/3;
			context.write(new Text( Float.toString(i) ), NullWritable.get());
		}
	}
	
	
	
	
	
	
	
	
	/*
	
	
	public static class TrianglesMapper extends Mapper<Object, Text, Text, IntWritable> {
	
		class SebSort { 

			public String[] sebSorted(String[] str) {
				
				for (int i = 0; i < 2; i++) {
					if (str[i].length() > str[i+1].length() || (str[i].length() == str[i+1].length() && str[i].compareTo(str[i+1]) > 0) ) {
						String tmp = str[i];
						str[i] = str[i+1];
						str[i+1] = tmp;

					}
				}
				
				for (int i = 2; i >0; i--) {
					if (str[i-1].length() > str[i].length() || (str[i-1].length() == str[i].length() && str[i-1].compareTo(str[i]) > 0) ) {
						String tmp = str[i];
						str[i] = str[i-1];
						str[i-1] = tmp;

					}
				}
				return str;
			}
		}

		private Text n;
		private SebSort seb;
	
		@Override
		protected void setup(Context context) {
			seb = new SebSort();
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				n = new Text( itr.nextToken() );
				String s = n.toString();
				String[] str = s.split("~",3);
				String[] z = seb.sebSorted(str);
				context.write(new Text( z[0] + "~" + z[1] + "~" + z[2] ), new IntWritable(1));
			}
		}
	}
	
	public static class TrianglesReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
		
		private int count;
	
		@Override
		protected void setup(Context context) {
			count = 0;
		}
	
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int total = 0;
			for (IntWritable value : values) {
				total++;
			}
			if (total >= 3) {
				count++;
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException { 
			context.write(new IntWritable(count), NullWritable.get());
		}
	}
	
	*/
	
	// Calculate Global Cluster Coefficient in the Driver.....
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		// Job #1
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Generate Triples");
		
		job.setJarByClass(Cluster.class);
		job.setMapperClass(Cluster.GenerateTriplesMapper.class);
		job.setReducerClass(Cluster.GenerateTriplesReducer.class);
		
		// There Is Only One Reducer
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
		job.waitForCompletion(true);
		
		
		
		
		// Job #2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Count Triples");
		
		job2.setJarByClass(Cluster.class);
		job2.setMapperClass(Cluster.CountTriplesMapper.class);
		job2.setReducerClass(Cluster.CountTriplesReducer.class);
		
		// There Is Only One Reducer
		job2.setNumReduceTasks(1);
		
		// Output for Mapper
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
		
		// Output for Reducer
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		// Input and Outpur Files
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		// Did The Job Complete?
		job2.waitForCompletion(true);
	
		
		
		// Job #3
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "Create & Count Triangles");
		
		job3.setJarByClass(Cluster.class);
		job3.setReducerClass(Cluster.CreateTrianglesReducer.class);
		
		// There Is Only One Reducer
		job3.setNumReduceTasks(1);
		
		// Output for Mapper
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job3, new Path(args[0]), TextInputFormat.class, Cluster.CreateTrianglesMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class, Cluster.GenerateTrianglesMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		// Did The Job Complete?
		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		
		
		
		/*
		// Job #4
		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "Count Triangles");
		
		job4.setJarByClass(Cluster.class);
		job4.setMapperClass(Cluster.TrianglesMapper.class);
		job4.setReducerClass(Cluster.TrianglesReducer.class);
		
		// There Is Only One Reducer
		job4.setNumReduceTasks(1);
		
		// Output for Mapper
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(IntWritable.class);
		
		// Output for Reducer
		job4.setOutputKeyClass(IntWritable.class);
		job4.setOutputValueClass(NullWritable.class);
		
		// Input and Outpur Files
		FileInputFormat.addInputPath(job4, new Path(args[1]));
		FileOutputFormat.setOutputPath(job4, new Path(args[3]));
		
		// Did The Job Complete?
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
		*/
	}
}




