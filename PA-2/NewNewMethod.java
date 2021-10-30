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


public class NewNewMethod {
	
	
	
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


	
	
	public static class LengthCMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// input: 2~56~8
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("C" + value.toString()));
		
		}
	}
	
	
	public static class LengthDMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("D" + value.toString()));
		
		}
	}


	
	
	public static class LengthEMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// input: 2~56~8
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("E" + value.toString()));
		
		}
	}
	
	
	public static class LengthFMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("F" + value.toString()));
		
		}
	}


	
	
	public static class LengthGMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// input: 2~56~8
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("G" + value.toString()));
		
		}
	}
	
	
	public static class LengthHMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("H" + value.toString()));
		
		}
	}


	
	
	public static class LengthIMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// input: 2~56~8
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("I" + value.toString()));
		
		}
	}
	
	
	public static class LengthJMapper extends Mapper<Object, Text, Text, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("J" + value.toString()));
		
		}
	}
	
	public static class AdjustmentReducer extends Reducer<Text, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String A = "", B = "",C = "", D = "",E = "", F = "",G = "", H = "",I = "", J = "";
			
			for (Text val : values) {
				String s = val.toString();
				if (s.charAt(0) == 'A') {
					A = s.substring(1);
				}
				else if (s.charAt(0) == 'B') {
					B = s.substring(1);
				}
				else if (s.charAt(0) == 'C') {
					C = s.substring(1);
				}
				else if (s.charAt(0) == 'D') {
					D = s.substring(1);
				}
				else if (s.charAt(0) == 'E') {
					E = s.substring(1);
				}
				else if (s.charAt(0) == 'F') {
					F = s.substring(1);
				}
				else if (s.charAt(0) == 'G') {
					G = s.substring(1);
				}
				else if (s.charAt(0) == 'H') {
					H = s.substring(1);
				}
				else if (s.charAt(0) == 'I') {
					I = s.substring(1);
				}
				else{
					J = s.substring(1);
				}
			}
			
			if (!A.isEmpty()) {
				context.write(new Text(A), NullWritable.get());
			}
			
			else if (!B.isEmpty()) {
				context.write(new Text(B), NullWritable.get());
			}
			
			else if (!C.isEmpty()) {
				context.write(new Text(C), NullWritable.get());
			}
			
			else if (!D.isEmpty()) {
				context.write(new Text(D), NullWritable.get());
			}
			
			else if (!E.isEmpty()) {
				context.write(new Text(E), NullWritable.get());
			}
			
			else if (!F.isEmpty()) {
				context.write(new Text(F), NullWritable.get());
			}
			
			else if (!G.isEmpty()) {
				context.write(new Text(G), NullWritable.get());
			}
			
			else if (!H.isEmpty()) {
				context.write(new Text(H), NullWritable.get());
			}
			
			else if (!I.isEmpty()) {
				context.write(new Text(I), NullWritable.get());
			}
			
			else {
				context.write(new Text(J), NullWritable.get());
			}
		}
	}
	
	
	
	
	
	
	
	
	public static class GetBMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetBReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 2 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetCMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetCReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 3 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetDMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetDReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 4 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetEMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetEReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 5 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetFMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetFReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 6 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetGMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetGReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 7 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetHMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetHReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 8 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	
	
	public static class GetIMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetIReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 9 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
			}
		}
	}
	
	
	
	public static class GetJMapper extends Mapper<Object, Text, IntWritable, Text> {
		
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] s = value.toString().split("~", 0);
			int len = s.length-1;
			context.write(new IntWritable(len), new Text(value.toString()));
		
		}
	}
	
	
	public static class GetJReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
		
		// s.substring(1); remove first char
		// input: <start~end, path>
		
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if (key.get() == 10 ) {
				for (Text T : values) {
					context.write(T, NullWritable.get());
				}
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
	
	
	
	
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		/*
		// Job #1 
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "Remove Duplicate Edges:");
		
		job1.setJarByClass(NewNewMethod.class);
		job1.setMapperClass(NewNewMethod.EdgesMapper.class);
		job1.setReducerClass(NewNewMethod.RemoveEdgesReducer.class);
		
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
		
		job2.setJarByClass(NewNewMethod.class);
		job2.setMapperClass(NewNewMethod.EdgesMapper.class);
		job2.setReducerClass(NewNewMethod.CountEdgesReducer.class);
		
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
		Job job3 = Job.getInstance(conf3, "Unrevised Length 2");
		
		job3.setJarByClass(NewNewMethod.class);
		job3.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job3.setNumReduceTasks(1);
		
		// Output for Mapper
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job3, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[2]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		// Did The Job Complete?
		job3.waitForCompletion(true);
		
		
		
		// Job #4
		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "Unrevised Length 3");
		
		job4.setJarByClass(NewNewMethod.class);
		job4.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job4.setNumReduceTasks(1);
		
		// Output for Mapper
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job4, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job4, new Path(args[3]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		
		// Did The Job Complete?
		job4.waitForCompletion(true);
		
		
		
		
		
		// Job #5
		Configuration conf5 = new Configuration();
		Job job5 = Job.getInstance(conf5, "Unrevised Length 4");
		
		job5.setJarByClass(NewNewMethod.class);
		job5.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job5.setNumReduceTasks(1);
		
		// Output for Mapper
		job5.setMapOutputKeyClass(Text.class);
		job5.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job5.setOutputKeyClass(Text.class);
		job5.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job5, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job5, new Path(args[4]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job5, new Path(args[5]));
		
		// Did The Job Complete?
		job5.waitForCompletion(true);
		
		
		
		// Job #6
		Configuration conf6 = new Configuration();
		Job job6 = Job.getInstance(conf6, "Unrevised Length 5");
		
		job6.setJarByClass(NewNewMethod.class);
		job6.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job6.setNumReduceTasks(1);
		
		// Output for Mapper
		job6.setMapOutputKeyClass(Text.class);
		job6.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job6.setOutputKeyClass(Text.class);
		job6.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job6, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job6, new Path(args[5]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job6, new Path(args[6]));
		
		// Did The Job Complete?
		job6.waitForCompletion(true);
		
		
		
		
		
		// Job #7
		Configuration conf7 = new Configuration();
		Job job7 = Job.getInstance(conf7, "Unrevised Length 6");
		
		job7.setJarByClass(NewNewMethod.class);
		job7.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job7.setNumReduceTasks(1);
		
		// Output for Mapper
		job7.setMapOutputKeyClass(Text.class);
		job7.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job7.setOutputKeyClass(Text.class);
		job7.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job7, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job7, new Path(args[6]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job7, new Path(args[7]));
		
		// Did The Job Complete?
		job7.waitForCompletion(true);
		
		
		
		
		
		
		
		
		
		// Job #8
		Configuration conf8 = new Configuration();
		Job job8 = Job.getInstance(conf8, "Unrevised Length 7");
		
		job8.setJarByClass(NewNewMethod.class);
		job8.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job8.setNumReduceTasks(1);
		
		// Output for Mapper
		job8.setMapOutputKeyClass(Text.class);
		job8.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job8.setOutputKeyClass(Text.class);
		job8.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job8, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job8, new Path(args[7]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job8, new Path(args[8]));
		
		// Did The Job Complete?
		job8.waitForCompletion(true);
		
		
		
		
		
		
		
		
		// Job #9
		Configuration conf9 = new Configuration();
		Job job9 = Job.getInstance(conf9, "Unrevised Length 8");
		
		job9.setJarByClass(NewNewMethod.class);
		job9.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job9.setNumReduceTasks(1);
		
		// Output for Mapper
		job9.setMapOutputKeyClass(Text.class);
		job9.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job9.setOutputKeyClass(Text.class);
		job9.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job9, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job9, new Path(args[8]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job9, new Path(args[9]));
		
		// Did The Job Complete?
		job9.waitForCompletion(true);
		
		
		
		
		
		
		
		// Job #10
		Configuration conf10 = new Configuration();
		Job job10 = Job.getInstance(conf10, "Unrevised Length 9");
		
		job10.setJarByClass(NewNewMethod.class);
		job10.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job10.setNumReduceTasks(1);
		
		// Output for Mapper
		job10.setMapOutputKeyClass(Text.class);
		job10.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job10.setOutputKeyClass(Text.class);
		job10.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job10, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job10, new Path(args[9]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job10, new Path(args[10]));
		
		// Did The Job Complete?
		job10.waitForCompletion(true);
		
		
		
		
		
		// Job #11
		Configuration conf11 = new Configuration();
		Job job11 = Job.getInstance(conf11, "Unrevised Length 10");
		
		job11.setJarByClass(NewNewMethod.class);
		job11.setReducerClass(NewNewMethod.PlusOneReducer.class);
		
		// There Is Only One Reducer
		job11.setNumReduceTasks(1);
		
		// Output for Mapper
		job11.setMapOutputKeyClass(Text.class);
		job11.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job11.setOutputKeyClass(Text.class);
		job11.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job11, new Path(args[1]), TextInputFormat.class, NewNewMethod.ListMapper.class);
		MultipleInputs.addInputPath(job11, new Path(args[10]), TextInputFormat.class, NewNewMethod.PreviousMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job11, new Path(args[11]));
		
		// Did The Job Complete?
		job11.waitForCompletion(true);
		
		
		
		
		// Job #12
		Configuration conf12 = new Configuration();
		Job job12 = Job.getInstance(conf12, "Adjustments");
		
		job12.setJarByClass(NewNewMethod.class);
		job12.setReducerClass(NewNewMethod.AdjustmentReducer.class);
		
		// There Is Only One Reducer
		job12.setNumReduceTasks(1);
		
		// Output for Mapper
		job12.setMapOutputKeyClass(Text.class);
		job12.setMapOutputValueClass(Text.class);
		
		// Output for Reducer
		job12.setOutputKeyClass(Text.class);
		job12.setOutputValueClass(NullWritable.class);
		
		
		MultipleInputs.addInputPath(job12, new Path(args[2]), TextInputFormat.class, NewNewMethod.LengthAMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[3]), TextInputFormat.class, NewNewMethod.LengthBMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[4]), TextInputFormat.class, NewNewMethod.LengthCMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[5]), TextInputFormat.class, NewNewMethod.LengthDMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[6]), TextInputFormat.class, NewNewMethod.LengthEMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[7]), TextInputFormat.class, NewNewMethod.LengthFMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[8]), TextInputFormat.class, NewNewMethod.LengthGMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[9]), TextInputFormat.class, NewNewMethod.LengthHMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[10]), TextInputFormat.class, NewNewMethod.LengthIMapper.class);
		MultipleInputs.addInputPath(job12, new Path(args[11]), TextInputFormat.class, NewNewMethod.LengthJMapper.class);
		
		
		// Output File
		FileOutputFormat.setOutputPath(job12, new Path(args[12]));
		
		// Did The Job Complete?
		job12.waitForCompletion(true);
		
		
		
		
		
		
		// Job #13
		Configuration conf13 = new Configuration();
		Job job13 = Job.getInstance(conf13, "Get Paths for Length 2");
		
		job13.setJarByClass(NewNewMethod.class);
		job13.setMapperClass(NewNewMethod.GetBMapper.class);
		job13.setReducerClass(NewNewMethod.GetBReducer.class);
		
		job13.setNumReduceTasks(1);
		
		job13.setMapOutputKeyClass(IntWritable.class);
		job13.setMapOutputValueClass(Text.class);
		
		job13.setOutputKeyClass(Text.class);
		job13.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job13, new Path(args[12]));
		FileOutputFormat.setOutputPath(job13, new Path(args[13]));
		
		job13.waitForCompletion(true);
		
		*/
		
		
		// Job #14
		Configuration conf14 = new Configuration();
		Job job14 = Job.getInstance(conf14, "Get Paths for Length 3");
		
		job14.setJarByClass(NewNewMethod.class);
		job14.setMapperClass(NewNewMethod.GetCMapper.class);
		job14.setReducerClass(NewNewMethod.GetCReducer.class);
		
		job14.setNumReduceTasks(1);
		
		job14.setMapOutputKeyClass(IntWritable.class);
		job14.setMapOutputValueClass(Text.class);
		
		job14.setOutputKeyClass(Text.class);
		job14.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job14, new Path(args[12]));
		FileOutputFormat.setOutputPath(job14, new Path(args[14]));
		
		job14.waitForCompletion(true);
		
		
		
		
		// Job #15
		Configuration conf15 = new Configuration();
		Job job15 = Job.getInstance(conf15, "Get Paths for Length 4");
		
		job15.setJarByClass(NewNewMethod.class);
		job15.setMapperClass(NewNewMethod.GetDMapper.class);
		job15.setReducerClass(NewNewMethod.GetDReducer.class);
		
		job15.setNumReduceTasks(1);
		
		job15.setMapOutputKeyClass(IntWritable.class);
		job15.setMapOutputValueClass(Text.class);
		
		job15.setOutputKeyClass(Text.class);
		job15.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job15, new Path(args[12]));
		FileOutputFormat.setOutputPath(job15, new Path(args[15]));
		
		job15.waitForCompletion(true);
		
		
		
		
		// Job #16
		Configuration conf16 = new Configuration();
		Job job16 = Job.getInstance(conf16, "Get Paths for Length 5");
		
		job16.setJarByClass(NewNewMethod.class);
		job16.setMapperClass(NewNewMethod.GetEMapper.class);
		job16.setReducerClass(NewNewMethod.GetEReducer.class);
		
		job16.setNumReduceTasks(1);
		
		job16.setMapOutputKeyClass(IntWritable.class);
		job16.setMapOutputValueClass(Text.class);
		
		job16.setOutputKeyClass(Text.class);
		job16.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job16, new Path(args[12]));
		FileOutputFormat.setOutputPath(job16, new Path(args[16]));
		
		job16.waitForCompletion(true);
		
		
		
		
		// Job #17
		Configuration conf17 = new Configuration();
		Job job17 = Job.getInstance(conf17, "Get Paths for Length 6");
		
		job17.setJarByClass(NewNewMethod.class);
		job17.setMapperClass(NewNewMethod.GetFMapper.class);
		job17.setReducerClass(NewNewMethod.GetFReducer.class);
		
		job17.setNumReduceTasks(1);
		
		job17.setMapOutputKeyClass(IntWritable.class);
		job17.setMapOutputValueClass(Text.class);
		
		job17.setOutputKeyClass(Text.class);
		job17.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job17, new Path(args[12]));
		FileOutputFormat.setOutputPath(job17, new Path(args[17]));
		
		job17.waitForCompletion(true);
		
		
		
		
		// Job #18
		Configuration conf18 = new Configuration();
		Job job18 = Job.getInstance(conf18, "Get Paths for Length 7");
		
		job18.setJarByClass(NewNewMethod.class);
		job18.setMapperClass(NewNewMethod.GetGMapper.class);
		job18.setReducerClass(NewNewMethod.GetGReducer.class);
		
		job18.setNumReduceTasks(1);
		
		job18.setMapOutputKeyClass(IntWritable.class);
		job18.setMapOutputValueClass(Text.class);
		
		job18.setOutputKeyClass(Text.class);
		job18.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job18, new Path(args[12]));
		FileOutputFormat.setOutputPath(job18, new Path(args[18]));
		
		job18.waitForCompletion(true);
		
		
		
		
		// Job #19
		Configuration conf19 = new Configuration();
		Job job19 = Job.getInstance(conf19, "Get Paths for Length 8");
		
		job19.setJarByClass(NewNewMethod.class);
		job19.setMapperClass(NewNewMethod.GetHMapper.class);
		job19.setReducerClass(NewNewMethod.GetHReducer.class);
		
		job19.setNumReduceTasks(1);
		
		job19.setMapOutputKeyClass(IntWritable.class);
		job19.setMapOutputValueClass(Text.class);
		
		job19.setOutputKeyClass(Text.class);
		job19.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job19, new Path(args[12]));
		FileOutputFormat.setOutputPath(job19, new Path(args[19]));
		
		job19.waitForCompletion(true);
		
		
		
		
		// Job #20
		Configuration conf20 = new Configuration();
		Job job20 = Job.getInstance(conf20, "Get Paths for Length 9");
		
		job20.setJarByClass(NewNewMethod.class);
		job20.setMapperClass(NewNewMethod.GetIMapper.class);
		job20.setReducerClass(NewNewMethod.GetIReducer.class);
		
		job20.setNumReduceTasks(1);
		
		job20.setMapOutputKeyClass(IntWritable.class);
		job20.setMapOutputValueClass(Text.class);
		
		job20.setOutputKeyClass(Text.class);
		job20.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job20, new Path(args[12]));
		FileOutputFormat.setOutputPath(job20, new Path(args[20]));
		
		job20.waitForCompletion(true);
		
		
		
		
		
		// Job #21
		Configuration conf21 = new Configuration();
		Job job21 = Job.getInstance(conf21, "Get Paths for Length 10");
		
		job21.setJarByClass(NewNewMethod.class);
		job21.setMapperClass(NewNewMethod.GetJMapper.class);
		job21.setReducerClass(NewNewMethod.GetJReducer.class);
		
		job21.setNumReduceTasks(1);
		
		job21.setMapOutputKeyClass(IntWritable.class);
		job21.setMapOutputValueClass(Text.class);
		
		job21.setOutputKeyClass(Text.class);
		job21.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job21, new Path(args[12]));
		FileOutputFormat.setOutputPath(job21, new Path(args[21]));
		
		job21.waitForCompletion(true);
		
	}
}
		
		




