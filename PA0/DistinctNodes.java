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

import com.google.common.collect.HashMultiset; 
import com.google.common.collect.Multiset; 

public class DistinctNodes {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text node ;
		private Text n;

		@Override
		//protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());   // this tokenizes (hashes) the string value
			
			while (itr.hasMoreTokens()) {

				String s = itr.nextToken();
				String r = itr.nextToken();
				
				if (s.length() > r.length() || (s.length() == r.length() && s.compareTo(r) > 0) ) {
					node = new Text( r );
					n = new Text( s );
				}
				
				else { // if (s.length() < r.length() || (s.length() == r.length() && r.compareTo(s) > 0) ) {
					node = new Text( s );
					n = new Text( r );
				}
				
				context.write(node, n); // Context object allows the map end to interact with the Hadoop System
			}
		}
	}
	
	//public static class CountReducer extends Reducer<Text, IntWritable, IntWritable, NullWritable> {
	//public static class CountReducer extends Reducer<Text, NullWritable, IntWritable, NullWritable> {
	public static class CountReducer extends Reducer<Text, Text, Text, Text> {
		
		//private Set<String> distinctNodes ;
		//private Map<String,Integer> distinctNodes ;
		private Multiset<String> distinctNodes;
		//private List<List<String>> distinctNodes;
		//private Map<String,Integer> distinctNodes;
	
		@Override
		protected void setup(Context context) { // this will only be called once
			distinctNodes = HashMultiset.create();
			//distinctNodes = new HashSet<String>();
			//distinctNodes = new HashMap<String,Integer>();
			//distinctNodes = new HashMap<>();
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{ // this will be called multiple times for each of the different keys
		//protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
			//distinctNodes.add(key.toString());
			
			for (Text value : values) {
				//context.write(key, value);
				distinctNodes.add(key.toString() + " " + value.toString());
			}
			
			//distinctNodes.put(key.getValue(),);
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException { // called once, last method to be called. it cleans things up
			//IntWritable noOfNodes = new IntWritable(distinctNodes.size());
			//String s[] = distinctNodes.toArray();
			
			
			//Text t = new Text((distinctNodes.values()).toArray());
			
			//Text t = new Text(Arrays.toString(Arrays.toString((distinctNodes.values()).toArray()));
			//context.write(t, NullWritable.get());
			
			//for (Map.Entry<String, String> entry : distinctNodes.entrySet()) {
			//	context.write( new Text(entry.getKey()), new Text(entry.getValue()) );
			//}
			
			int i = 0;
			
			Iterable<Multiset.Entry<String>> tmp = Multisets.copyHighestCountFirst(distinctNodes).entrySet();
			
			/*
			for (Multiset.Entry<String> entry : distinctNodes.entrySet()){
				context.write(new Text( entry.getElement() ), new Text( Integer.toString(entry.getCount()) ));
				
				i++;
				if (i == 100) {
					break;
				}
			}
			*/
			
			for (Multiset.Entry<String> entry : tmp){
				context.write(new Text(i + " " + entry.getElement() ), new Text( Integer.toString(entry.getCount()) ));
				
				i++;
				if (i == 100) {
					break;
				}
			}
			
			//context.write(noOfNodes, NullWritable.get());
			//IntWritable noOf = new IntWritable(distinctNodes.count("1"));
			//context.write(noOf, NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// Create Hadoop Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "distinct nodes new");
		
		job.setJarByClass(DistinctNodes.class);
		job.setMapperClass(DistinctNodes.TokenizerMapper.class);
		job.setReducerClass(DistinctNodes.CountReducer.class);
		
		// There Is Only On Reducer
		job.setNumReduceTasks(1);
		
		// Output for Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Input and Outpur Files
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// Did The Job Complete?
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

// Task 1: count the number of edges
//          - there wont be any duplictes, only directed graphs. No undirected graphs are here.
// Task 2: count the in-degree and out-degree of distinct vertices
//          - generated two sorted lists (sorted in descending order)
//                 - In-degree list
//                 - Out-degree list
//                 - only submit the first 100 vertices in each list
//          - measure the in-degree and out-degree of each distinct vertex
// Taske 3: Extracting the "Friends" network
//          - 


// for the chain <key, value>
// key = the node number
// value = in-degree or out-degree

// Task 2 & 3 will be there own separate files
// Do Task 1 on this file


// Each task will be tested with a different output
// When testing your code make sure the path to the files are correctly written in the functions


