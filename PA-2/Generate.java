public class Generate {
	
	
	public static void main(String args[]) {
		ArrayList<ArrayList<>>
	}
}


key = v1

val1 = v2
val2 = v5

context.write(val1~key~val2)

val1 = v5
val2 = v6

context.write(val1~key~val2)

	public static class CountReducer extends Reducer<Text, Text, Text, Text> {

		

		@Override
		protected void setup(Context context) {
			
		}
	
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String one = "";
			String two = "";
			for (Text value : values) {
				
				context.write(val1~key~val2, NullWritable)
			}
		}
	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException { 
			
		}
	}
