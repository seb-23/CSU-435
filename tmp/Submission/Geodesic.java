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



public class Geodesic {

    public static class ListMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int r = Integer.parseInt(context.getConfiguration().get("R_Value"));
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                String s = itr.nextToken();
                String c = itr.nextToken();

                context.write(new Text(s), new Text(c));
                if (r != 1) {
					context.write(new Text(c), new Text(s));
				}
            }
        }
    }

    public static class GeneratePathsMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int r = Integer.parseInt(context.getConfiguration().get("R_Value"));
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {

                String str = itr.nextToken();

                String[] s = str.split("~");
                String reverse = "";
                for (int i = 0; i < s.length; i++){
                    reverse += s[s.length - i-1] + "~";
                }

                reverse = reverse.substring(0,reverse.length()-1);
                
                context.write(new Text(s[0]), new Text(reverse));
                context.write(new Text(s[s.length - 1]), new Text(str));
            }
        }
    }


//    public static class GeneratePathsMapper extends Mapper<Object, Text, Text, Text> {
//
//        private Text n;
//        private Text m;
//
//        @Override
//        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//
//            while (itr.hasMoreTokens()) {
//
//                String s = itr.nextToken();
//                m = new Text( s );
//                String[] t = s.split("~");
//                n = new Text( t[t.length-1] );
//
//                context.write(n, m);
//                
//            }
//        }
//    }
//

    public static class GeneratePathsReducer extends Reducer<Text, Text, Text, NullWritable> {


        class Combination {

            public void combinationUtil(ArrayList<String> s, String data[], int start, int end, int index, ArrayList<String> out, int r) {
                if (index == r) {
                    for (int j=0; j<r; j++) {
                        out.add(data[j]);
                    }
                    return;
                }

                for (int i=start; i<=end && end-i+1 >= r-index; i++) {
                    data[index] = s.get(i);
                    combinationUtil(s, data, i+1, end, index+1, out, r);
                }
            }

            public void Combinations(ArrayList<String> s, int n, ArrayList<String> out, int r) {
                String data[]=new String[r];
                combinationUtil(s, data, 0, n-1, 0, out, r);
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
            int r = Integer.parseInt(context.getConfiguration().get("R_Value"));

            if (r < 3) {
                nodes.clear();
                for (Text value : values) {
                    nodes.add(value.toString());
                }

                ArrayList<String> out = new ArrayList<String>();
                combo.Combinations(nodes, nodes.size(), out, r);
                for (int j = 0; j < out.size()/r; j++) {
                    String s = out.get(r*j) + "~" + key.toString();
                    for (int i = 1; i < r; i++) {
                        s += "~" + out.get(r*j+i);
                    }

                    context.write(new Text(s), NullWritable.get());
                }
            }
            else {

                ArrayList<String> P = new ArrayList<String>(); // <7, 1~8~7>
                ArrayList<String> L = new ArrayList<String>(); // <7, [1,4,3,8]>

                for (Text val : values) {
                    String s = val.toString();
                    if (s.contains("~")) {
                        P.add(s); // [1~8~7]
                    }
                    else {
                        L.add(s); // [1,4,3,8]
                    }
                }

                for (String A : P) {
                    String[] S = A.split("~", 0);
                    for (String B : L) {
                        int truth = 0;
                        for (String C : S) {
                            if (C.equals(B)) {
                                truth += 1;
                            }
                        }
                        if (truth==0) {
                            context.write(new Text( A+"~"+B ), NullWritable.get());
                        }
                    }
                }
            }
        }
    }



    public static class LengthAMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // input example: 2~56~8
            String[] s = value.toString().split("~");
            context.write(new Text(s[0] + "~" + s[s.length-1]), new Text("A" + value.toString()));
            context.write(new Text(s[s.length-1] + "~" + s[0]), new Text("A" + value.toString()));
        }
    }


    public static class LengthBMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] v = value.toString().split("~");
            int last = v.length-1;

            if (v[last].length() > v[0].length() || (v[last].length() == v[0].length() && v[last].compareTo(v[0]) > 0)) {
                context.write(new Text(v[0] + "~" + v[last]), new Text("B" + value.toString()));
            } else {
                context.write(new Text(v[last] + "~" + v[0]), new Text("B" + value.toString()));
            }
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









    public static void main(String[] args) throws Exception {
		
		
		
	int a = 11;

        for (int i = 1; i <= 10; i++) {
            if (i < 3) {
                Configuration conf = new Configuration();
                conf.set("R_Value", Integer.toString(i));
                Job job1 = Job.getInstance(conf, "Generate Length: " + Integer.toString(i));

                job1.setJarByClass(Geodesic.class);
                job1.setMapperClass(Geodesic.ListMapper.class);
                job1.setReducerClass(Geodesic.GeneratePathsReducer.class);

                // There Is Only One Reducer
                job1.setNumReduceTasks(10);

                // Output for Mapper
                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);

                // Output for Reducer
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(NullWritable.class);

                FileInputFormat.addInputPath(job1, new Path(args[0]));
                if (i==1) {
                    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
                }
                else {
                    FileOutputFormat.setOutputPath(job1, new Path(args[a]));
                }

                // Did The Job Complete?
                job1.waitForCompletion(true);
            }
            else {
                Configuration conf8 = new Configuration();
                conf8.set("R_Value", Integer.toString(i));
                Job job = Job.getInstance(conf8, "Generate Length: " + Integer.toString(i));

                job.setJarByClass(Geodesic.class);
                job.setReducerClass(Geodesic.GeneratePathsReducer.class);

                // There Is Only One Reducer
                job.setNumReduceTasks(10);

                // Output for Mapper
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                // Output for Reducer
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Geodesic.ListMapper.class);
                MultipleInputs.addInputPath(job, new Path(args[i-1]), TextInputFormat.class, Geodesic.GeneratePathsMapper.class);

                FileOutputFormat.setOutputPath(job, new Path(args[a]));

                // Did The Job Complete?
                job.waitForCompletion(true);
            }


            for (int j = 2; j <= i; j++) {

                // Job #4
                Configuration conf4 = new Configuration();
                Job jobN = Job.getInstance(conf4, "Length Adjustment: " + Integer.toString(i) + " using: " + Integer.toString(j-1));

                jobN.setJarByClass(Geodesic.class);
                jobN.setReducerClass(Geodesic.LengthAdjustmentReducer.class);

                // There Is Only One Reducer
                jobN.setNumReduceTasks(10);

                // Output for Mapper
                jobN.setMapOutputKeyClass(Text.class);
                jobN.setMapOutputValueClass(Text.class);

                // Output for Reducer
                jobN.setOutputKeyClass(Text.class);
                jobN.setOutputValueClass(NullWritable.class);


                MultipleInputs.addInputPath(jobN, new Path(args[j - 1]), TextInputFormat.class, Geodesic.LengthAMapper.class);
                MultipleInputs.addInputPath(jobN, new Path(args[a++]), TextInputFormat.class, Geodesic.LengthBMapper.class);


                // Output File
                if (i == j) {
                    FileOutputFormat.setOutputPath(jobN, new Path(args[i]));
                } else {
                    FileOutputFormat.setOutputPath(jobN, new Path(args[a]));
                }

                // Did The Job Complete?
                jobN.waitForCompletion(true);
            }
        }	
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		/*

        //int a = 4;
        int a = 11;
        //int a = 5;

        //for (int i = 1; i <= 3; i++) {
        for (int i = 1; i <= 10; i++) {
        //for (int i = 1; i <= 4; i++) {
            if (i < 3) {
                Configuration conf = new Configuration();
                conf.set("R_Value", Integer.toString(i));
                Job job1 = Job.getInstance(conf, "Generate Length: " + Integer.toString(i));

                job1.setJarByClass(Geodesic.class);
                job1.setMapperClass(Geodesic.ListMapper.class);
                job1.setReducerClass(Geodesic.GeneratePathsReducer.class);

                // There Is Only One Reducer
                job1.setNumReduceTasks(100);

                // Output for Mapper
                job1.setMapOutputKeyClass(Text.class);
                job1.setMapOutputValueClass(Text.class);

                // Output for Reducer
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(NullWritable.class);

                FileInputFormat.addInputPath(job1, new Path(args[0]));
                FileOutputFormat.setOutputPath(job1, new Path(args[a++]));

                // Did The Job Complete?
                job1.waitForCompletion(true);
            }
            else {
                Configuration conf8 = new Configuration();
                conf8.set("R_Value", Integer.toString(i));
                Job job = Job.getInstance(conf8, "Generate Length: " + Integer.toString(i));

                job.setJarByClass(Geodesic.class);
                job.setReducerClass(Geodesic.GeneratePathsReducer.class);

                // There Is Only One Reducer
                job.setNumReduceTasks(100);

                // Output for Mapper
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                // Output for Reducer
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(NullWritable.class);

                MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Geodesic.ListMapper.class);
                MultipleInputs.addInputPath(job, new Path(args[a-1]), TextInputFormat.class, Geodesic.GeneratePathsMapper.class);

                FileOutputFormat.setOutputPath(job, new Path(args[a++]));

                // Did The Job Complete?
                job.waitForCompletion(true);
            }
        }





  
        for (int k = 10; k > 1; k--) {
        //for (int k = 4; k > 1; k--) {
            int truth = 1;
            for (int j = k+10-1; j >= 10+1; j--) {
            //for (int j = k+4-1; j >= 4+1; j--) {

                // Job #4
                Configuration conf4 = new Configuration();
                Job jobN = Job.getInstance(conf4, "Length Adjustment: " + Integer.toString(k) + " using: " + Integer.toString(j - 1));

                jobN.setJarByClass(Geodesic.class);
                jobN.setReducerClass(Geodesic.LengthAdjustmentReducer.class);

                // There Is Only One Reducer
                jobN.setNumReduceTasks(100);

                // Output for Mapper
                jobN.setMapOutputKeyClass(Text.class);
                jobN.setMapOutputValueClass(Text.class);

                // Output for Reducer
                jobN.setOutputKeyClass(Text.class);
                jobN.setOutputValueClass(NullWritable.class);


                MultipleInputs.addInputPath(jobN, new Path(args[j]), TextInputFormat.class, Geodesic.LengthAMapper.class);
                
                if (truth == 1) {
                    //MultipleInputs.addInputPath(jobN, new Path(args[k + 3]), TextInputFormat.class, Geodesic.LengthBMapper.class);
                    MultipleInputs.addInputPath(jobN, new Path(args[k + 10]), TextInputFormat.class, Geodesic.LengthBMapper.class);
                    //MultipleInputs.addInputPath(jobN, new Path(args[k + 4]), TextInputFormat.class, Geodesic.LengthBMapper.class);
                    truth = 0;
                }
                else {
                    MultipleInputs.addInputPath(jobN, new Path(args[a++]), TextInputFormat.class, Geodesic.LengthBMapper.class);
                }

                // Output File
                if ( j == 10+1 ) {
                //if ( j == 4+1 ) {
                    FileOutputFormat.setOutputPath(jobN, new Path(args[k]));
                } else {
                    FileOutputFormat.setOutputPath(jobN, new Path(args[a]));
                }

                // Did The Job Complete?
                jobN.waitForCompletion(true);
            }
        }
        
        
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Last One");

		job1.setJarByClass(Geodesic.class);
		job1.setMapperClass(Geodesic.LengthBMapper.class);
		job1.setReducerClass(Geodesic.LengthAdjustmentReducer.class);

		// There Is Only One Reducer
		job1.setNumReduceTasks(1);

		// Output for Mapper
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		// Output for Reducer
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(NullWritable.class);

		//FileInputFormat.addInputPath(job1, new Path(args[3+1]));
		FileInputFormat.addInputPath(job1, new Path(args[10+1]));
		//FileInputFormat.addInputPath(job1, new Path(args[4+1]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		// Did The Job Complete?
		job1.waitForCompletion(true);
		*/
        
    }
}





