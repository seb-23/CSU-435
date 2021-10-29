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

public class Task2_OutDegree {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text node ;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); 
			
			while (itr.hasMoreTokens()) {
				String s = itr.nextToken();
				node = new Text( s );
				
				itr.nextToken();

				context.write(node, new Text("1"));
			}
		}
	}
	
	
	public static class CountReducer extends Reducer<Text, Text, Text, Text> {
		
		private Multiset<String> distinctNodes;
	
		@Override
		protected void setup(Context context) { 
			distinctNodes = HashMultiset.create();
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text value : values) {
				distinctNodes.add(key.toString());
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			int i = 0;
			
			Iterable<Multiset.Entry<String>> tmp = Multisets.copyHighestCountFirst(distinctNodes).entrySet();
			
			for (Multiset.Entry<String> entry : tmp){
				context.write(new Text(entry.getElement() ), new Text( Integer.toString(entry.getCount()) ));
				
				i++;
				if (i == 100) {
					break;
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		// Create Hadoop Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "distinct nodes new");
		
		job.setJarByClass(Task2_OutDegree.class);
		job.setMapperClass(Task2_OutDegree.TokenizerMapper.class);
		job.setReducerClass(Task2_OutDegree.CountReducer.class);
		
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
