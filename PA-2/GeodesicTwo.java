
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

import java.io.*; 
import java.util.List;
import java.util.ArrayList;
import java.util.*; 

import com.google.common.collect.HashMultiset; 
import com.google.common.collect.Multiset; 


public class GeodesicTwo {
	
	
	// Job #1
	
	public static class AdjListMapper extends Mapper<Object, Text, Text, Text> {
	
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
	
	// Job #1
	
	// Mapper Input: whole data record
	// Mapper Output: <Node 1, Node 2>
	// Shuffle & Sort Output: AdjList
	// TestReducer Input: AdjList
	// TestReducer Outut: <Sub-Graph, NullWritable>

	public static class TestReducer extends Reducer<Text, Text, Text, NullWritable> {
		class test { 

			private ArrayList< ArrayList<String> > cluster;
			private ArrayList<String> indices;
			private ArrayList< ArrayList<String> > ali;

			test() { 
				cluster = new ArrayList<ArrayList<String>>();
				indices = new ArrayList<String>();
				ali = new ArrayList<ArrayList<String>>();
			} 

			public void addEdge( String src, String dest) {		
				if (indices.indexOf(src) == -1) {
					indices.add(src);
					cluster.add(new ArrayList<String>());
					cluster.get( cluster.size()-1 ).add(dest);
				}
				else {
					if (cluster.get(indices.indexOf(src)).indexOf(dest) == -1) {
						cluster.get(indices.indexOf(src)).add(dest);
					}
				}
				
				if (indices.indexOf(dest) == -1) {
					indices.add(dest);
					cluster.add(new ArrayList<String>());
					cluster.get( cluster.size()-1 ).add(src);
				}
				else {
					if (cluster.get(indices.indexOf(dest)).indexOf(src) == -1) {
						cluster.get(indices.indexOf(dest)).add(src);
					}
				}
			}
			
			public void DFSUtil(int v, ArrayList<String> visited) {
				visited.set(v, "true");
				ali.get(ali.size()-1).add(indices.get(v));
				
				for (String s : cluster.get(v)) {
					int x = indices.indexOf(s);
					if (x != -1) {
						if(visited.get(x) == "false") DFSUtil(x,visited);
					}
				}
			}
			
			public ArrayList< ArrayList<String> > connectedComponents() { 
				int len = indices.size();
				ArrayList<String> visited = new ArrayList<String>();
				for (int i = 0; i < len; i++){
					visited.add("false");
				}

				for(int v = 0; v < len; ++v) { 
					if (ali.isEmpty()) {
						ali.add(new ArrayList<String>());
					}
					else if (ali.get(ali.size()-1).size() > 0) {
						ali.add(new ArrayList<String>());
					}
					if(visited.get(v) == "false") { 
						DFSUtil(v,visited);
					}
				}
				
				if (ali.get(ali.size()-1).size() == 0) {
					ali.remove(ali.size()-1);
				}
				return ali;
			}
		}
		private test tezt;
	
		@Override
		protected void setup(Context context) {
			tezt = new test();
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text val : values) {
				tezt.addEdge( key.toString(), val.toString() );
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			ArrayList< ArrayList<String> > t = tezt.connectedComponents();
			
			for (int i = 0; i < t.size(); i++) {
				String s = "";
				for (int j = 0; j < t.get(i).size(); j++) {
					s += t.get(i).get(j) + " ";
				}
				context.write(new Text( s ), NullWritable.get() );
			}
		}
	}
	
	
	
	
	
	
	
	// Job #2
	
	public static class CombinationMapper extends Mapper<Object, Text, Text, Text> {
		
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
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			nodes.clear();
			
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				nodes.add(itr.nextToken());
			}
			ArrayList<String> out = new ArrayList<String>();
			combo.Combinations(nodes, nodes.size(), out);
			for (int j = 0; j < out.size()/2; j++) {
				context.write(new Text(out.get(2*j)), new Text("C" + out.get(2*j+1)));
			}
		}
	}
	
	public static class AdjancenyListMapper extends Mapper<Object, Text, Text, Text> {
	
		private Text n;
		private Text m;

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				
				n = new Text( itr.nextToken() );
				m = new Text( "A" + itr.nextToken() );
				
				context.write(n, m);
			}
		}
	}
	
	
	// Job #2
	// AdjListMapper Input: whole record data adjList
	// AdjListMapper Output: adjList + "A"
	// CombinationMapper Input: Sub-Graph + "S" <2,7,4,5,2,9,0> ...
	// CombinationMapper Output: <source, Destination> <2,7> <2,4> <2,5>... <7,4> <7,5> <7,2> Combinations 
	
	// GraphReducer Input: <startNode, [List of endNodes]> & <adjList> (Last Element = "S" || "A") store both in ArrayLists
		// Feed AdjList to Graph.java
		// call printAllPaths() using <startNode, endNode>
		// Path Length <= 10    (Number of ~ <= 9 || Number of Digits <= 10)
	// GraphReducer Output: <startNode~endNode, Path>
	
	public static class GraphReducer extends Reducer<Text, Text, IntWritable, Text> {
		
		class Graph { 
		  
			private ArrayList< ArrayList<String> > adjList; 
			private ArrayList<String> indices;
			private Set<String> paths;
		  
			// Constructor 
			public Graph() {   
				adjList = new ArrayList< ArrayList<String> >();
				indices = new ArrayList<String>();
				paths = new HashSet<String>();
			}
			
			public String getShortestPath() {
				int min = 11;
				String path = "";
				
				for (String s : paths) {
					int len = 0;
					for (char c : s.toCharArray()) {
						if (c == '~') {
							len++;
						}
					}
					
					if (len==1) {
						return s;
					}
					
					if (len < min) {
						min = len;
						path = s;
					}
				}
				
				
				return path;
			}
		  
			public void addEdge(String u, String v) { 
				if (indices.indexOf(u) == -1) {
					indices.add(u);
					adjList.add(new ArrayList<String>());
					adjList.get(indices.indexOf(u)).add(v);
				}
				else{
					if (adjList.get(indices.indexOf(u)).indexOf(v) == -1) {
						adjList.get(indices.indexOf(u)).add(v);
					}
				}
				
				if (indices.indexOf(v) == -1) {
					indices.add(v);
					adjList.add(new ArrayList<String>());
					adjList.get(indices.indexOf(v)).add(u);
				}
				else{
					if (adjList.get(indices.indexOf(v)).indexOf(u) == -1) {
						adjList.get(indices.indexOf(v)).add(u);
					}
				}
			} 
		  
		 
			public void AllPaths(String s, String d) {  
				ArrayList<String> pathList = new ArrayList<String>(); 
				pathList.add(s); 

				boolean[] isVisited = new boolean[ indices.size() ];
				AllPathsUtil(s, d, isVisited, pathList); 
			} 
		  
			private void AllPathsUtil(String u, String d, boolean[] isVisited, ArrayList<String> localPathList) { 
		  
				if (localPathList.size() > 3) {
					return;
				}
				
				if (u.equals(d)) { 
					String str = "";
					for (String element : localPathList) {
						str += element + "~";
					}
					paths.add(str.substring(0, str.length()-1));
					return; 
				} 
				
				if (indices.indexOf(u) == -1) {
					return;
				}
				isVisited[ indices.indexOf(u) ] = true; 
		  
				for (String s : adjList.get( indices.indexOf(u) )) {
					int i = indices.indexOf(s);
					if (!isVisited[i]) { 
						localPathList.add(s); 
						AllPathsUtil(s, d, isVisited, localPathList); 
		  
						localPathList.remove( localPathList.indexOf(s) ); 
					}
				} 
		 
				isVisited[ indices.indexOf(u) ] = false; 
			}  
		}
		
		private Graph g;
		private ArrayList<ArrayList<String>> fromto;
	
		@Override
		protected void setup(Context context) {
			g = new Graph();
			fromto = new ArrayList<ArrayList<String>>();
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text val : values) {
				if (val.charAt(0) == 'A') {
					g.addEdge( key.toString(), val.toString().substring(1, val.toString().length()) );
				}
				else {
					fromto.add(new ArrayList<String>());
					fromto.get(fromto.size()-1).add(key.toString());
					fromto.get(fromto.size()-1).add(val.toString().substring(1, val.toString().length()));
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			
			for (int i = 0; i < fromto.size(); i++) {
				String k = fromto.get(i).get(0);
				String v = fromto.get(i).get(1);
				g.AllPaths(k, v);
			}
			
			String p = g.getShortestPath();
			int len = 0;
			for (char c : p.toCharArray()) {
				if (c == '~') {
					len++;
				}
			}
			context.write(new IntWritable(len), new Text( p ));
		}
	}
	
	
	
	
	/*
	
	// Job #3
	
	public static class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {
	
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
	
	// Job #3
	// ShortestPathMapper Input: whole record data <startNode~endNode, Path>
	// ShortestPathMapper Output: <startNode~endNode, Path>
	// ShortestPathReducer Input: <startNode~endNode, [List of Paths]>
	// ShortestPathReducer Output: <Path Length, Shortest Path w/ Nodes>
	
	public static class ShortestPathReducer extends Reducer<Text, Text, IntWritable, Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int shortestPathLength = 101;
			String shortestPath = "";
			
			for (Text val : values) {
				int len = 0;
				for (char c : val.toString().toCharArray()) {
					if (c == '~') {
						len++;
					}
				}
				if (len < shortestPathLength) {
					shortestPathLength = len;
					shortestPath = val.toString();
				}
			}

			context.write( new IntWritable( shortestPathLength ), new Text( shortestPath ) );
		}
	}
	
	
	*/
	
	
	
	// Job #4
	
	// Job #4: This is a guess... remember you have to use the formula
	// Mapper Input: <Path length, Shortest Path w/ Nodes>
	// Mapper Output: <Shortest Path Length, 1> <IntWritable, INtWritable>
	
	// Mapper Input: <Length, Shortest Path w/ Nodes>
	// Mapper Output: <Length, Shortest Path w/ Nodes> <IntWritable, INtWritable>
	
	public static class FinaleMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while (itr.hasMoreTokens()) {
				
				int n = Integer.parseInt(itr.nextToken());
				itr.nextToken();
				
				context.write(new IntWritable(n), new IntWritable(1));
			}
		}
	}
	
	// Job $4
	
	// Reducer Input: <Shortest Path Length, [List of 1s]>
	// Reducer Output: <Path Length(1-10), local sum of lengths>
	
	public static class FinaleReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
		//private ArrayList<int[]> total;
		private ArrayList< ArrayList<Integer> > total;
	
		@Override
		protected void setup(Context context) {
			//total = new ArrayList<int[]>();
			total = new ArrayList< ArrayList<Integer> >();
		}
	
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			for (IntWritable val : values) {
				count++;
			}
			//total.add( new int[]{key.get(), count} );
			total.add(new ArrayList<Integer>());
			total.get(total.size()-1).add(key.get());
			total.get(total.size()-1).add(count);
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (int i = 0; i < total.size(); i++) {
				sum += total.get(i).get(1);
				context.write(new IntWritable( total.get(i).get(0) ), new IntWritable( total.get(i).get(1) ) );
			}
			/*
			for (int[] i : total) {
				sum += i[1];
				context.write(new IntWritable(i[0]), new IntWritable(i[1]));
			}
			*/
		}
	}
		
		
		
		
	
	public static void main(String[] args) throws Exception {
		
		// Job #1
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find Sub-Graphs");
		
		job.setJarByClass(GeodesicTwo.class);
		job.setMapperClass(GeodesicTwo.AdjListMapper.class);
		job.setReducerClass(GeodesicTwo.TestReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
		
		// Job #2
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Find <startNode~endNode, Path>");
		
		job2.setJarByClass(GeodesicTwo.class);
		job2.setReducerClass(GeodesicTwo.GraphReducer.class);
		
		job2.setNumReduceTasks(1);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, GeodesicTwo.AdjancenyListMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, GeodesicTwo.CombinationMapper.class);
		
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.waitForCompletion(true);
		
		

		/*
		// Job #3
		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "Find ShortestPath");
		
		job3.setJarByClass(GeodesicTwo.class);
		job3.setMapperClass(GeodesicTwo.ShortestPathMapper.class);
		job3.setReducerClass(GeodesicTwo.ShortestPathReducer.class);
		
		job3.setNumReduceTasks(1);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job3, new Path(args[2]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		
		job3.waitForCompletion(true);
		
		*/
		
		
		// Job #4
		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "Returns Path Lengths & Their Sums");
		
		job4.setJarByClass(GeodesicTwo.class);
		job4.setMapperClass(GeodesicTwo.FinaleMapper.class);
		job4.setReducerClass(GeodesicTwo.FinaleReducer.class);
		
		job4.setNumReduceTasks(1);
		
		job4.setMapOutputKeyClass(IntWritable.class);
		job4.setMapOutputValueClass(IntWritable.class);
		
		job4.setOutputKeyClass(IntWritable.class);
		job4.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job4, new Path(args[2]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
		
		System.exit(job4.waitForCompletion(true) ? 0 : 1);
		
		
	}
}




	// Job #1
	// Mapper Input: whole data record
	// Mapper Output: <Node 1, Node 2>
	// Shuffle & Sort Output: AdjList
	// TestReducer Input: AdjList
	// TestReducer Outut: <NullWritable, Sub-Graph>
	
	
	
	// Job #2
	// AdjListMapper Input: whole record data adjList
	// AdjListMapper Output: adjList + "A"
	// CombinationMapper Input: Sub-Graph + "S" <2,7,4,5,2,9,0> ...
	// CombinationMapper Output: <source, Destination> <2,7> <2,4> <2,5>... <7,4> <7,5> <7,2> Combinations 
	
	// GraphReducer Input: <startNode, [List of endNodes]> & <adjList> (Last Element = "S" || "A") store both in ArrayLists
				// Feed AdjList to Graph.java
				// call printAllPaths() using <startNode, endNode>
				// Path Length <= 10    (Number of ~ <= 9 || Number of Digits <= 10)
	// GraphReducer Output: <startNode~endNode, Path>
	
	
	
	// Job #3
	// ShortestPathMapper Input: whole record data <startNode~endNode, Path>
	// ShortestPathMapper Output: <startNode~endNode, Path>
	// ShortestPathReducer Input: <startNode~endNode, [List of Paths]>
	// ShortestPathReducer Output: <startNode~endNode, Shortest Path w/ Nodes>
	
	
	
	// Job #4: This is a guess... remember you have to use the formula
	// Mapper Input: <startNode~endNode, Shortest Path w/ Nodes>
	// Mapper Output: <Shortest Path Length, 1> <IntWritable, INtWritable>
	// Reducer Input: <Shortest Path Length, [List of 1s]>
	// Reducer Output: <NullWritable, GeodesicTwo Average>
