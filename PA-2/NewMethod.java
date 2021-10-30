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


public class NewMethod {
	
	
	
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
	
	public static class CountEdgesReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private ArrayList<String> distinctEdges;
		
		@Override
		protected void setup(Context context) {
			distinctEdges = new ArrayList<String>();
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) {
			for (Text val : values) {
				String v = val.toString();
				String s = key.toString();
				
				if (s.length() > v.length() || (s.length() == v.length() && s.compareTo(v) > 0) ) {
					
					if (distinctEdges.indexOf(v + "~" + s) == -1) {
						distinctEdges.add(v + "~" + s);
					}
				}
				
				else {
					if (distinctEdges.indexOf(s + "~" + v) == -1) {
						distinctEdges.add(s + "~" + v);
					}
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < distinctEdges.size(); i++) {
				context.write(new Text( distinctEdges.get(i) ), NullWritable.get());
			}
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
				String[] str = s.split("~",0);
				context.write(new Text(str[0]), new Text(str[1]));
			}
		}
	}
	
	
	











	
	
	public static class LengthAMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// input: 2~56~8
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("A" + value.toString()));
		
		}
	}
	
	
	public static class LengthBMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("B" + value.toString()));
		
		}
	}
	
	public static class LengthAdjustmentReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String A = "", B = "";
			
			for (Text val : values) {
				String s = val.toString();
				if (s.charAt(0) == 'A') {
					A = s.substring(1);
				}
				else {
					B = s.substring(1);
				}
			}
			
			if (A.isEmpty()) {
				context.write(new Text(B), NullWritable.get());
			}
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static class ListMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {

				String s = itr.nextToken();
				String c = itr.nextToken();
				
				context.write(new Text(s), new Text("L" + c));
				context.write(new Text(c), new Text("L" + s));
			}
		}
		
	}
	
	
	public static class PreviousMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// input: 4~9~1~3
			String[] s = value.toString().split("~", 0);
			
			byte[] strAsByteArray = value.toString().getBytes();
			byte[] result = new byte[strAsByteArray.length];
	 
			for (int i = 0; i < strAsByteArray.length; i++){
				result[i] = strAsByteArray[strAsByteArray.length - i - 1];
			}
				
			String reverse = new String(result);
			
			context.write(new Text(s[0]), new Text("P" + reverse ));  // <4, 3~1~9~4>
			context.write(new Text(s[s.length-1]), new Text("P" + value.toString())); // <3, 4~9~1~3>
		}
	}
	
	
	public static class PlusOneReducer extends Reducer<Text, Text, Text, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<String> P = new ArrayList<String>();
			ArrayList<String> L = new ArrayList<String>();
			
			for (Text val : values) {
				String s = val.toString();
				if (s.charAt(0) == 'P') {
					P.add(s.substring(1));
				}
				else {
					L.add(s.substring(1));
				}
			}
			
			ArrayList<String> C = new ArrayList<String>();
			
			for (String A : P) {
				String[] S = A.split("~", 0);
				
				inner:
				for (String B : L) {
					
					for (String N : S) {
						if (N.equals(B)) {
							break inner;
						}
					}
					
					if (C.indexOf(S[S.length-1]) == -1) {
						context.write(new Text( A+"~"+B ), NullWritable.get());
						C.add(S[S.length-1]);
					}
				}
			}
		}
	}
	
	
	
	

	
	
	/*
	public static class PlusOneReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		private ArrayList<String> P;
		private ArrayList<String> L;
		
		@Override
		protected void setup(Context context) {
			P = new ArrayList<String>();
			L = new ArrayList<String>();
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) {
			
			for (Text val : values) {
				String s = val.toString();
				if (s.charAt(0) == 'P') {
					P.add(s.substring(1));
				}
				else {
					L.add(s.substring(1));
				}
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException { 
			for (String A : P) {
				String[] S = A.split("~", 0);
				
				for (String B : L) {
					
					context.write(new Text( A+"~"+B ), NullWritable.get());
				}
			}
		}
	}
	*/
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		
		// Job #1 
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "Remove Duplicate Edges:");
		
		job1.setJarByClass(NewMethod.class);
		job1.setMapperClass(NewMethod.EdgesMapper.class);
		job1.setReducerClass(NewMethod.RemoveEdgesReducer.class);
		
		job1.setNumReduceTasks(1);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		job1.waitForCompletion(true);
		
		
		// Job #2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Length 1");
		
		job2.setJarByClass(NewMethod.class);
		job2.setMapperClass(NewMethod.EdgesMapper.class);
		job2.setReducerClass(NewMethod.CountEdgesReducer.class);
		
		job2.setNumReduceTasks(1);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		job2.waitForCompletion(true);
		
		
		
		
		
		
		// Job #3
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "Length 2");
		
		job3.setJarByClass(NewMethod.class);
		job3.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job3.setNumReduceTasks(1);
		
		// Output for Mapper
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[2]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		// Did The Job Complete?
		job3.waitForCompletion(true);
		
		
		
		// Job #4
		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "Length 2 Adjustment");
		
		job4.setJarByClass(NewMethod.class);
		job4.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job4.setNumReduceTasks(1);
		
		// Output for Mapper
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job4, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job4, new Path(args[3]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		
		// Did The Job Complete?
		job4.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
		// Job #5
		Configuration conf5 = new Configuration();
		Job job5 = Job.getInstance(conf5, "Length 3");
		
		job5.setJarByClass(NewMethod.class);
		job5.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job5.setNumReduceTasks(1);
		
		// Output for Mapper
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job5, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job5, new Path(args[4]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job5, new Path(args[5]));
		
		// Did The Job Complete?
		job5.waitForCompletion(true);
		
		
		
		// Job #6
		Configuration conf6 = new Configuration();
		Job job6 = Job.getInstance(conf6, "Length 3 Adjustment w/ 2");
		
		job6.setJarByClass(NewMethod.class);
		job6.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job6.setNumReduceTasks(1);
		
		// Output for Mapper
		job6.setMapOutputKeyClass(Text.class);
		job6.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job6, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job6, new Path(args[5]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job6, new Path(args[6]));
		
		// Did The Job Complete?
		job6.waitForCompletion(true);
		
		
		
		
		
		// Job #7
		Configuration conf7 = new Configuration();
		Job job7 = Job.getInstance(conf7, "Length 3 Adjustment w/ 1");
		
		job7.setJarByClass(NewMethod.class);
		job7.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job7.setNumReduceTasks(1);
		
		// Output for Mapper
		job7.setMapOutputKeyClass(Text.class);
		job7.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job7.setOutputKeyClass(Text.class);
		job7.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job7, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job7, new Path(args[6]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job7, new Path(args[7]));
		
		// Did The Job Complete?
		job7.waitForCompletion(true);
		
		
		
		
		// Job #8
		Configuration conf8 = new Configuration();
		Job job8 = Job.getInstance(conf8, "Unrevised Length 4");
		
		job8.setJarByClass(NewMethod.class);
		job8.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job8.setNumReduceTasks(1);
		
		// Output for Mapper
		job8.setMapOutputKeyClass(Text.class);
		job8.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job8.setOutputKeyClass(Text.class);
		job8.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job8, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job8, new Path(args[7]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job8, new Path(args[8]));
		
		// Did The Job Complete?
		job8.waitForCompletion(true);
		
		
		
		// Job #9
		Configuration conf9 = new Configuration();
		Job job9 = Job.getInstance(conf9, "Length 4 Adjustment w/ 3");
		
		job9.setJarByClass(NewMethod.class);
		job9.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job9.setNumReduceTasks(1);
		
		// Output for Mapper
		job9.setMapOutputKeyClass(Text.class);
		job9.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job9, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job9, new Path(args[8]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job9, new Path(args[9]));
		
		// Did The Job Complete?
		job9.waitForCompletion(true);
		
		
		
		
		
		// Job #10
		Configuration conf10 = new Configuration();
		Job job10 = Job.getInstance(conf10, "Length 4 Adjustment w/ 2");
		
		job10.setJarByClass(NewMethod.class);
		job10.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job10.setNumReduceTasks(1);
		
		// Output for Mapper
		job10.setMapOutputKeyClass(Text.class);
		job10.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job10.setOutputKeyClass(Text.class);
		job10.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job10, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job10, new Path(args[9]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job10, new Path(args[10]));
		
		// Did The Job Complete?
		job10.waitForCompletion(true);
		
		
		
		// Job #11
		Configuration conf11 = new Configuration();
		Job job11 = Job.getInstance(conf11, "Final Length 4");
		
		job11.setJarByClass(NewMethod.class);
		job11.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job11.setNumReduceTasks(1);
		
		// Output for Mapper
		job11.setMapOutputKeyClass(Text.class);
		job11.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job11.setOutputKeyClass(Text.class);
		job11.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job11, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job11, new Path(args[10]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job11, new Path(args[11]));
		
		// Did The Job Complete?
		job11.waitForCompletion(true);
		
		
		
		
		
		//------------------------------------------------------------
		
		
		
		
		// Job #12
		Configuration conf12 = new Configuration();
		Job job12 = Job.getInstance(conf12, "Unrevised Length 5");
		
		job12.setJarByClass(NewMethod.class);
		job12.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job12.setNumReduceTasks(1);
		
		// Output for Mapper
		job12.setMapOutputKeyClass(Text.class);
		job12.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job12.setOutputKeyClass(Text.class);
		job12.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job12, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[11]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job12, new Path(args[12]));
		
		// Did The Job Complete?
		job12.waitForCompletion(true);
		
		
		
		// Job #13
		Configuration conf13 = new Configuration();
		Job job13 = Job.getInstance(conf13, "Length 5 Adjustment w/ 4");
		
		job13.setJarByClass(NewMethod.class);
		job13.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job13.setNumReduceTasks(1);
		
		// Output for Mapper
		job13.setMapOutputKeyClass(Text.class);
		job13.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job13.setOutputKeyClass(Text.class);
		job13.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job13, new Path(args[11]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job13, new Path(args[12]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job13, new Path(args[13]));
		
		// Did The Job Complete?
		job13.waitForCompletion(true);
		
		
		
		
		
		// Job #14
		Configuration conf14 = new Configuration();
		Job job14 = Job.getInstance(conf14, "Length 5 Adjustment w/ 3");
		
		job14.setJarByClass(NewMethod.class);
		job14.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job14.setNumReduceTasks(1);
		
		// Output for Mapper
		job14.setMapOutputKeyClass(Text.class);
		job14.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job14.setOutputKeyClass(Text.class);
		job14.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job14, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job14, new Path(args[13]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job14, new Path(args[14]));
		
		// Did The Job Complete?
		job14.waitForCompletion(true);
		
		
		
		// Job #15
		Configuration conf15 = new Configuration();
		Job job15 = Job.getInstance(conf15, "Length 5 Adjustment w/ 2");
		
		job15.setJarByClass(NewMethod.class);
		job15.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job15.setNumReduceTasks(1);
		
		// Output for Mapper
		job15.setMapOutputKeyClass(Text.class);
		job15.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job15.setOutputKeyClass(Text.class);
		job15.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job15, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job15, new Path(args[14]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job15, new Path(args[15]));
		
		// Did The Job Complete?
		job15.waitForCompletion(true);
		
		
		
		// Job #16
		Configuration conf16 = new Configuration();
		Job job16 = Job.getInstance(conf16, "Final Length 5");
		
		job16.setJarByClass(NewMethod.class);
		job16.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job16.setNumReduceTasks(1);
		
		// Output for Mapper
		job16.setMapOutputKeyClass(Text.class);
		job16.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job16.setOutputKeyClass(Text.class);
		job16.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job16, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job16, new Path(args[15]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job16, new Path(args[16]));
		
		// Did The Job Complete?
		job16.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
		
		// Job #17
		Configuration conf17 = new Configuration();
		Job job17 = Job.getInstance(conf17, "Unrevised Length 6");
		
		job17.setJarByClass(NewMethod.class);
		job17.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job17.setNumReduceTasks(1);
		
		// Output for Mapper
		job17.setMapOutputKeyClass(Text.class);
		job17.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job17.setOutputKeyClass(Text.class);
		job17.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job17, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job17, new Path(args[16]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job17, new Path(args[17]));
		
		// Did The Job Complete?
		job17.waitForCompletion(true);
		
		
		
		// Job #18
		Configuration conf18 = new Configuration();
		Job job18 = Job.getInstance(conf18, "Length 6 Adjustment w/ 5");
		
		job18.setJarByClass(NewMethod.class);
		job18.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job18.setNumReduceTasks(1);
		
		// Output for Mapper
		job18.setMapOutputKeyClass(Text.class);
		job18.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job18.setOutputKeyClass(Text.class);
		job18.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job18, new Path(args[16]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job18, new Path(args[17]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job18, new Path(args[18]));
		
		// Did The Job Complete?
		job18.waitForCompletion(true);
		
		
		
		
		
		// Job #19
		Configuration conf19 = new Configuration();
		Job job19 = Job.getInstance(conf19, "Length 6 Adjustment w/ 4");
		
		job19.setJarByClass(NewMethod.class);
		job19.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job19.setNumReduceTasks(1);
		
		// Output for Mapper
		job19.setMapOutputKeyClass(Text.class);
		job19.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job19.setOutputKeyClass(Text.class);
		job19.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job19, new Path(args[11]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job19, new Path(args[18]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job19, new Path(args[19]));
		
		// Did The Job Complete?
		job19.waitForCompletion(true);
		
		
		
		// Job #20
		Configuration conf20 = new Configuration();
		Job job20 = Job.getInstance(conf20, "Length 6 Adjustment w/ 3");
		
		job20.setJarByClass(NewMethod.class);
		job20.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job20.setNumReduceTasks(1);
		
		// Output for Mapper
		job20.setMapOutputKeyClass(Text.class);
		job20.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job20.setOutputKeyClass(Text.class);
		job20.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job20, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job20, new Path(args[19]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job20, new Path(args[20]));
		
		// Did The Job Complete?
		job20.waitForCompletion(true);
		
		
		
		// Job #21
		Configuration conf21 = new Configuration();
		Job job21 = Job.getInstance(conf21, "Length 6 Adjustment w/ 2");
		
		job21.setJarByClass(NewMethod.class);
		job21.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job21.setNumReduceTasks(1);
		
		// Output for Mapper
		job21.setMapOutputKeyClass(Text.class);
		job21.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job21.setOutputKeyClass(Text.class);
		job21.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job21, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job21, new Path(args[20]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job21, new Path(args[21]));
		
		// Did The Job Complete?
		job21.waitForCompletion(true);
		
		
		
		
		// Job #22
		Configuration conf22 = new Configuration();
		Job job22 = Job.getInstance(conf22, "Final Length 6");
		
		job22.setJarByClass(NewMethod.class);
		job22.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job22.setNumReduceTasks(1);
		
		// Output for Mapper
		job22.setMapOutputKeyClass(Text.class);
		job22.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job22.setOutputKeyClass(Text.class);
		job22.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job22, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job22, new Path(args[21]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job22, new Path(args[22]));
		
		// Did The Job Complete?
		job22.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
		
		// Job #23
		Configuration conf23 = new Configuration();
		Job job23 = Job.getInstance(conf23, "Unrevised Length 7");
		
		job23.setJarByClass(NewMethod.class);
		job23.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job23.setNumReduceTasks(1);
		
		// Output for Mapper
		job23.setMapOutputKeyClass(Text.class);
		job23.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job23.setOutputKeyClass(Text.class);
		job23.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job23, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job23, new Path(args[22]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job23, new Path(args[23]));
		
		// Did The Job Complete?
		job23.waitForCompletion(true);
		
		
		
		// Job #24
		Configuration conf24 = new Configuration();
		Job job24 = Job.getInstance(conf24, "Length 7 Adjustment w/ 6");
		
		job24.setJarByClass(NewMethod.class);
		job24.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job24.setNumReduceTasks(1);
		
		// Output for Mapper
		job24.setMapOutputKeyClass(Text.class);
		job24.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job24.setOutputKeyClass(Text.class);
		job24.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job24, new Path(args[22]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job24, new Path(args[23]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job24, new Path(args[24]));
		
		// Did The Job Complete?
		job24.waitForCompletion(true);
		
		
		
		
		
		// Job #25
		Configuration conf25 = new Configuration();
		Job job25 = Job.getInstance(conf25, "Length 7 Adjustment w/ 5");
		
		job25.setJarByClass(NewMethod.class);
		job25.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job25.setNumReduceTasks(1);
		
		// Output for Mapper
		job25.setMapOutputKeyClass(Text.class);
		job25.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job25.setOutputKeyClass(Text.class);
		job25.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job25, new Path(args[16]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job25, new Path(args[24]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job25, new Path(args[25]));
		
		// Did The Job Complete?
		job25.waitForCompletion(true);
		
		
		
		// Job #26
		Configuration conf26 = new Configuration();
		Job job26 = Job.getInstance(conf26, "Length 7 Adjustment w/ 4");
		
		job26.setJarByClass(NewMethod.class);
		job26.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job26.setNumReduceTasks(1);
		
		// Output for Mapper
		job26.setMapOutputKeyClass(Text.class);
		job26.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job26.setOutputKeyClass(Text.class);
		job26.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job26, new Path(args[11]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job26, new Path(args[25]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job26, new Path(args[26]));
		
		// Did The Job Complete?
		job26.waitForCompletion(true);
		
		
		
		
		// Job #27
		Configuration conf27 = new Configuration();
		Job job27 = Job.getInstance(conf27, "Length 7 Adjustment w/ 3");
		
		job27.setJarByClass(NewMethod.class);
		job27.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job27.setNumReduceTasks(1);
		
		// Output for Mapper
		job27.setMapOutputKeyClass(Text.class);
		job27.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job27.setOutputKeyClass(Text.class);
		job27.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job27, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job27, new Path(args[26]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job27, new Path(args[27]));
		
		// Did The Job Complete?
		job27.waitForCompletion(true);
		
		
		
		
		// Job #28
		Configuration conf28 = new Configuration();
		Job job28 = Job.getInstance(conf28, "Length 7 Adjustment w/ 2");
		
		job28.setJarByClass(NewMethod.class);
		job28.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job28.setNumReduceTasks(1);
		
		// Output for Mapper
		job28.setMapOutputKeyClass(Text.class);
		job28.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job28.setOutputKeyClass(Text.class);
		job28.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job28, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job28, new Path(args[27]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job28, new Path(args[28]));
		
		// Did The Job Complete?
		job28.waitForCompletion(true);
		
		
		
		
		// Job #29
		Configuration conf29 = new Configuration();
		Job job29 = Job.getInstance(conf29, "Final Length 7");
		
		job29.setJarByClass(NewMethod.class);
		job29.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job29.setNumReduceTasks(1);
		
		// Output for Mapper
		job29.setMapOutputKeyClass(Text.class);
		job29.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job29.setOutputKeyClass(Text.class);
		job29.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job29, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job29, new Path(args[28]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job29, new Path(args[29]));
		
		// Did The Job Complete?
		job29.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
		
		// Job #30
		Configuration conf30 = new Configuration();
		Job job30 = Job.getInstance(conf30, "Unrevised Length 8");
		
		job30.setJarByClass(NewMethod.class);
		job30.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job30.setNumReduceTasks(1);
		
		// Output for Mapper
		job30.setMapOutputKeyClass(Text.class);
		job30.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job30.setOutputKeyClass(Text.class);
		job30.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job30, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job30, new Path(args[29]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job30, new Path(args[30]));
		
		// Did The Job Complete?
		job30.waitForCompletion(true);
		
		
		
		// Job #31
		Configuration conf31 = new Configuration();
		Job job31 = Job.getInstance(conf31, "Length 8 Adjustment w/ 7");
		
		job31.setJarByClass(NewMethod.class);
		job31.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job31.setNumReduceTasks(1);
		
		// Output for Mapper
		job31.setMapOutputKeyClass(Text.class);
		job31.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job31.setOutputKeyClass(Text.class);
		job31.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job31, new Path(args[29]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job31, new Path(args[30]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job31, new Path(args[31]));
		
		// Did The Job Complete?
		job31.waitForCompletion(true);
		
		
		
		
		
		// Job #32
		Configuration conf32 = new Configuration();
		Job job32 = Job.getInstance(conf32, "Length 8 Adjustment w/ 6");
		
		job32.setJarByClass(NewMethod.class);
		job32.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job32.setNumReduceTasks(1);
		
		// Output for Mapper
		job32.setMapOutputKeyClass(Text.class);
		job32.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job32.setOutputKeyClass(Text.class);
		job32.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job32, new Path(args[22]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job32, new Path(args[31]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job32, new Path(args[32]));
		
		// Did The Job Complete?
		job32.waitForCompletion(true);
		
		
		
		// Job #33
		Configuration conf33 = new Configuration();
		Job job33 = Job.getInstance(conf33, "Length 8 Adjustment w/ 5");
		
		job33.setJarByClass(NewMethod.class);
		job33.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job33.setNumReduceTasks(1);
		
		// Output for Mapper
		job33.setMapOutputKeyClass(Text.class);
		job33.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job33.setOutputKeyClass(Text.class);
		job33.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job33, new Path(args[16]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job33, new Path(args[32]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job33, new Path(args[33]));
		
		// Did The Job Complete?
		job33.waitForCompletion(true);
		
		
		
		
		
		// Job #34
		Configuration conf34 = new Configuration();
		Job job34 = Job.getInstance(conf34, "Length 8 Adjustment w/ 4");
		
		job34.setJarByClass(NewMethod.class);
		job34.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job34.setNumReduceTasks(1);
		
		// Output for Mapper
		job34.setMapOutputKeyClass(Text.class);
		job34.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job34.setOutputKeyClass(Text.class);
		job34.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job34, new Path(args[11]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job34, new Path(args[33]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job34, new Path(args[34]));
		
		// Did The Job Complete?
		job34.waitForCompletion(true);
		
		
		
		
		// Job #35
		Configuration conf35 = new Configuration();
		Job job35 = Job.getInstance(conf35, "Length 8 Adjustment w/ 3");
		
		job35.setJarByClass(NewMethod.class);
		job35.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job35.setNumReduceTasks(1);
		
		// Output for Mapper
		job35.setMapOutputKeyClass(Text.class);
		job35.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job35.setOutputKeyClass(Text.class);
		job35.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job35, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job35, new Path(args[34]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job35, new Path(args[35]));
		
		// Did The Job Complete?
		job35.waitForCompletion(true);
		
		
		
		
		// Job #36
		Configuration conf36 = new Configuration();
		Job job36 = Job.getInstance(conf36, "Length 8 Adjustment w/ 2");
		
		job36.setJarByClass(NewMethod.class);
		job36.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job36.setNumReduceTasks(1);
		
		// Output for Mapper
		job36.setMapOutputKeyClass(Text.class);
		job36.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job36.setOutputKeyClass(Text.class);
		job36.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job36, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job36, new Path(args[35]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job36, new Path(args[36]));
		
		// Did The Job Complete?
		job36.waitForCompletion(true);
		
		
		// Job #37
		Configuration conf37 = new Configuration();
		Job job37 = Job.getInstance(conf37, "Final Length 8");
		
		job37.setJarByClass(NewMethod.class);
		job37.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job37.setNumReduceTasks(1);
		
		// Output for Mapper
		job37.setMapOutputKeyClass(Text.class);
		job37.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job37.setOutputKeyClass(Text.class);
		job37.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job37, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job37, new Path(args[36]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job37, new Path(args[37]));
		
		// Did The Job Complete?
		job37.waitForCompletion(true);
		
		
		
		
		
		
		
		
		// ----------------------------------------------------------------------
		
		
		
		
		
		
		
		
		
		
		// Job #38
		Configuration conf38 = new Configuration();
		Job job38 = Job.getInstance(conf38, "Unrevised Length 9");
		
		job38.setJarByClass(NewMethod.class);
		job38.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job38.setNumReduceTasks(1);
		
		// Output for Mapper
		job38.setMapOutputKeyClass(Text.class);
		job38.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job38.setOutputKeyClass(Text.class);
		job38.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job38, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job38, new Path(args[37]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job38, new Path(args[38]));
		
		// Did The Job Complete?
		job38.waitForCompletion(true);
		
		
		
		// Job #39
		Configuration conf39 = new Configuration();
		Job job39 = Job.getInstance(conf39, "Length 9 Adjustment w/ 8");
		
		job39.setJarByClass(NewMethod.class);
		job39.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job39.setNumReduceTasks(1);
		
		// Output for Mapper
		job39.setMapOutputKeyClass(Text.class);
		job39.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job39.setOutputKeyClass(Text.class);
		job39.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job39, new Path(args[37]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job39, new Path(args[38]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job39, new Path(args[39]));
		
		// Did The Job Complete?
		job39.waitForCompletion(true);
		
		
		
		// Job #40
		Configuration conf40 = new Configuration();
		Job job40 = Job.getInstance(conf40, "Length 9 Adjustment w/ 7");
		
		job40.setJarByClass(NewMethod.class);
		job40.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job40.setNumReduceTasks(1);
		
		// Output for Mapper
		job40.setMapOutputKeyClass(Text.class);
		job40.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job40.setOutputKeyClass(Text.class);
		job40.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job40, new Path(args[29]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job40, new Path(args[39]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job40, new Path(args[40]));
		
		// Did The Job Complete?
		job40.waitForCompletion(true);
		
		
		
		
		
		// Job #41
		Configuration conf41 = new Configuration();
		Job job41 = Job.getInstance(conf41, "Length 9 Adjustment w/ 6");
		
		job41.setJarByClass(NewMethod.class);
		job41.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job41.setNumReduceTasks(1);
		
		// Output for Mapper
		job41.setMapOutputKeyClass(Text.class);
		job41.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job41.setOutputKeyClass(Text.class);
		job41.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job41, new Path(args[22]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job41, new Path(args[40]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job41, new Path(args[41]));
		
		// Did The Job Complete?
		job41.waitForCompletion(true);
		
		
		
		// Job #42
		Configuration conf42 = new Configuration();
		Job job42 = Job.getInstance(conf42, "Length 9 Adjustment w/ 5");
		
		job42.setJarByClass(NewMethod.class);
		job42.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job42.setNumReduceTasks(1);
		
		// Output for Mapper
		job42.setMapOutputKeyClass(Text.class);
		job42.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job42.setOutputKeyClass(Text.class);
		job42.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job42, new Path(args[16]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job42, new Path(args[41]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job42, new Path(args[42]));
		
		// Did The Job Complete?
		job42.waitForCompletion(true);
		
		
		
		
		
		// Job #43
		Configuration conf43 = new Configuration();
		Job job43 = Job.getInstance(conf43, "Length 9 Adjustment w/ 4");
		
		job43.setJarByClass(NewMethod.class);
		job43.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job43.setNumReduceTasks(1);
		
		// Output for Mapper
		job43.setMapOutputKeyClass(Text.class);
		job43.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job43.setOutputKeyClass(Text.class);
		job43.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job43, new Path(args[11]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job43, new Path(args[42]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job43, new Path(args[43]));
		
		// Did The Job Complete?
		job43.waitForCompletion(true);
		
		
		
		
		// Job #44
		Configuration conf44 = new Configuration();
		Job job44 = Job.getInstance(conf44, "Length 9 Adjustment w/ 3");
		
		job44.setJarByClass(NewMethod.class);
		job44.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job44.setNumReduceTasks(1);
		
		// Output for Mapper
		job44.setMapOutputKeyClass(Text.class);
		job44.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job44.setOutputKeyClass(Text.class);
		job44.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job44, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job44, new Path(args[43]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job44, new Path(args[44]));
		
		// Did The Job Complete?
		job44.waitForCompletion(true);
		
		
		
		
		// Job #45
		Configuration conf45 = new Configuration();
		Job job45 = Job.getInstance(conf45, "Length 9 Adjustment w/ 2");
		
		job45.setJarByClass(NewMethod.class);
		job45.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job45.setNumReduceTasks(1);
		
		// Output for Mapper
		job45.setMapOutputKeyClass(Text.class);
		job45.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job45.setOutputKeyClass(Text.class);
		job45.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job45, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job45, new Path(args[44]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job45, new Path(args[45]));
		
		// Did The Job Complete?
		job45.waitForCompletion(true);
		
		
		// Job #46
		Configuration conf46 = new Configuration();
		Job job46 = Job.getInstance(conf46, "Final Length 9");
		
		job46.setJarByClass(NewMethod.class);
		job46.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job46.setNumReduceTasks(1);
		
		// Output for Mapper
		job46.setMapOutputKeyClass(Text.class);
		job46.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job46.setOutputKeyClass(Text.class);
		job46.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job46, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job46, new Path(args[45]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job46, new Path(args[46]));
		
		// Did The Job Complete?
		job46.waitForCompletion(true);
		
		
		
		
		
		//---------------------------------------------------------------------------------------------
		
		
		
		
		
		
		
		// Job #47
		Configuration conf47 = new Configuration();
		Job job47 = Job.getInstance(conf47, "Unrevised Length 10");
		
		job47.setJarByClass(NewMethod.class);
		job47.setReducerClass(NewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job47.setNumReduceTasks(1);
		
		// Output for Mapper
		job47.setMapOutputKeyClass(Text.class);
		job47.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job47.setOutputKeyClass(Text.class);
		job47.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job47, new Path(args[1]), TextInputFormat.class, NewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job47, new Path(args[46]), TextInputFormat.class, NewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job47, new Path(args[47]));
		
		// Did The Job Complete?
		job47.waitForCompletion(true);
		
		
		
		
		// Job #48
		Configuration conf48 = new Configuration();
		Job job48 = Job.getInstance(conf48, "Length 10 Adjustment w/ 9");
		
		job48.setJarByClass(NewMethod.class);
		job48.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job48.setNumReduceTasks(1);
		
		// Output for Mapper
		job48.setMapOutputKeyClass(Text.class);
		job48.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job48.setOutputKeyClass(Text.class);
		job48.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job48, new Path(args[46]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job48, new Path(args[47]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job48, new Path(args[48]));
		
		// Did The Job Complete?
		job48.waitForCompletion(true);
		
		
		
		// Job #49
		Configuration conf49 = new Configuration();
		Job job49 = Job.getInstance(conf49, "Length 10 Adjustment w/ 8");
		
		job49.setJarByClass(NewMethod.class);
		job49.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job49.setNumReduceTasks(1);
		
		// Output for Mapper
		job49.setMapOutputKeyClass(Text.class);
		job49.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job49.setOutputKeyClass(Text.class);
		job49.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job49, new Path(args[37]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job49, new Path(args[48]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job49, new Path(args[49]));
		
		// Did The Job Complete?
		job49.waitForCompletion(true);
		
		
		
		// Job #50
		Configuration conf50 = new Configuration();
		Job job50 = Job.getInstance(conf50, "Length 10 Adjustment w/ 7");
		
		job50.setJarByClass(NewMethod.class);
		job50.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job50.setNumReduceTasks(1);
		
		// Output for Mapper
		job50.setMapOutputKeyClass(Text.class);
		job50.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job50.setOutputKeyClass(Text.class);
		job50.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job50, new Path(args[29]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job50, new Path(args[49]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job50, new Path(args[50]));
		
		// Did The Job Complete?
		job50.waitForCompletion(true);
		
		
		
		
		
		// Job #51
		Configuration conf51 = new Configuration();
		Job job51 = Job.getInstance(conf51, "Length 10 Adjustment w/ 6");
		
		job51.setJarByClass(NewMethod.class);
		job51.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job51.setNumReduceTasks(1);
		
		// Output for Mapper
		job51.setMapOutputKeyClass(Text.class);
		job51.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job51.setOutputKeyClass(Text.class);
		job51.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job51, new Path(args[22]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job51, new Path(args[50]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job51, new Path(args[51]));
		
		// Did The Job Complete?
		job51.waitForCompletion(true);
		
		
		
		// Job #52
		Configuration conf52 = new Configuration();
		Job job52 = Job.getInstance(conf52, "Length 10 Adjustment w/ 5");
		
		job52.setJarByClass(NewMethod.class);
		job52.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job52.setNumReduceTasks(1);
		
		// Output for Mapper
		job52.setMapOutputKeyClass(Text.class);
		job52.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job52.setOutputKeyClass(Text.class);
		job52.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job52, new Path(args[16]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job52, new Path(args[51]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job52, new Path(args[52]));
		
		// Did The Job Complete?
		job52.waitForCompletion(true);
		
		
		
		
		
		// Job #53
		Configuration conf53 = new Configuration();
		Job job53 = Job.getInstance(conf53, "Length 10 Adjustment w/ 4");
		
		job53.setJarByClass(NewMethod.class);
		job53.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job53.setNumReduceTasks(1);
		
		// Output for Mapper
		job53.setMapOutputKeyClass(Text.class);
		job53.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job53.setOutputKeyClass(Text.class);
		job53.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job53, new Path(args[11]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job53, new Path(args[52]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job53, new Path(args[53]));
		
		// Did The Job Complete?
		job53.waitForCompletion(true);
		
		
		
		
		// Job #54
		Configuration conf54 = new Configuration();
		Job job54 = Job.getInstance(conf54, "Length 10 Adjustment w/ 3");
		
		job54.setJarByClass(NewMethod.class);
		job54.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job54.setNumReduceTasks(1);
		
		// Output for Mapper
		job54.setMapOutputKeyClass(Text.class);
		job54.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job54.setOutputKeyClass(Text.class);
		job54.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job54, new Path(args[7]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job54, new Path(args[53]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job54, new Path(args[54]));
		
		// Did The Job Complete?
		job54.waitForCompletion(true);
		
		
		
		
		// Job #55
		Configuration conf55 = new Configuration();
		Job job55 = Job.getInstance(conf55, "Length 10 Adjustment w/ 2");
		
		job55.setJarByClass(NewMethod.class);
		job55.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job55.setNumReduceTasks(1);
		
		// Output for Mapper
		job55.setMapOutputKeyClass(Text.class);
		job55.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job55.setOutputKeyClass(Text.class);
		job55.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job55, new Path(args[4]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job55, new Path(args[54]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job55, new Path(args[55]));
		
		// Did The Job Complete?
		job55.waitForCompletion(true);
		
		
		// Job #56
		Configuration conf56 = new Configuration();
		Job job56 = Job.getInstance(conf56, "Final Length 10");
		
		job56.setJarByClass(NewMethod.class);
		job56.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		// There Is Only One Reducer
		job56.setNumReduceTasks(1);
		
		// Output for Mapper
		job56.setMapOutputKeyClass(Text.class);
		job56.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job56.setOutputKeyClass(Text.class);
		job56.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job56, new Path(args[2]), TextInputFormat.class, NewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job56, new Path(args[55]), TextInputFormat.class, NewMethod.LengthBMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job56, new Path(args[56]));
		
		// Did The Job Complete?
		System.exit(job56.waitForCompletion(true) ? 0 : 1);
		
		
		
	}
}
		
		
		
	// PlusOneReducer
	//private int len;
	/*
	Code for finding current path length when all lengths are 
	* present in the dataset
	for (String G : P) {
		String[] T = G.split("~", 0);
		if (T.length-1 > len) {
			len = T.length-1;
		}
	}
	
	//if (S.length - 1 == len) {
	
	
	
	 * 
	 * 1st make sure to remove duplicates!!!!
	 *     you already have a function that does this


	  then do this...
		for (String N : Y) {
			String[] H = N.split("~", 0);
			for (String W : H)
			if (W.equals(H[H.length])) {
				context.write(new Text( A+"~"+B ), NullWritable.get());
			}
		}
	  
	 */
		
		
		
		/*
		// Job #8
		Configuration conf8 = new Configuration();
		Job job8 = Job.getInstance(conf8, "Length 4");
		
		job8.setJarByClass(NewMethod.class);
		job8.setReducerClass(NewMethod.Length3.class);
		
		job8.setNumReduceTasks(1);
		
		job8.setMapOutputKeyClass(Text.class);
		job8.setMapOutputValueClass(Text.class);
		
		job8.setOutputKeyClass(Text.class);
		job8.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job8, new Path(args[1]), TextInputFormat.class, NewMethod.AdjacentListMapper.class);
		MultipleInputs.addInputPath(job8, new Path(args[5]), TextInputFormat.class, NewMethod.Length3AdjustedMapper.class);
		
		
		FileOutputFormat.setOutputPath(job8, new Path(args[8]));
		
		job8.waitForCompletion(true);
		
		
		
		// Job #9
		Configuration conf9 = new Configuration();
		Job job9 = Job.getInstance(conf9, "Length 4 Adjustment");
		
		job9.setJarByClass(NewMethod.class);
		job9.setReducerClass(NewMethod.LengthAdjustmentReducer.class);
		
		job9.setNumReduceTasks(1);
		
		job9.setMapOutputKeyClass(Text.class);
		job9.setMapOutputValueClass(Text.class);
		
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job9, new Path(args[8]), TextInputFormat.class, NewMethod.Length4Mapper.class);
		MultipleInputs.addInputPath(job9, new Path(args[6]), TextInputFormat.class, NewMethod.Length3Mapper.class);
		
		
		FileOutputFormat.setOutputPath(job9, new Path(args[9]));
		
		job9.waitForCompletion(true);
		
		


		
		// Job #1
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Generate Length 2");
		
		job.setJarByClass(NewMethod.class);
		job.setMapperClass(NewMethod.GenerateTriplesMapper.class);
		job.setReducerClass(NewMethod.GenerateTriplesReducer.class);
		
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
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		
		// Did The Job Complete?
		job.waitForCompletion(true);
		*/




