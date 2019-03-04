import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BDAssign3 {

	

		public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
			@Override
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString(),",");
				String batsman = itr.nextToken();
				String bowler = itr.nextToken();
				String pair = new String(batsman + "_" + bowler);
				int wicket = Integer.parseInt(itr.nextToken());
				context.write(new Text(pair), new IntWritable(wicket));
			}
		}

		public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
			private IntWritable result = new IntWritable();
			@Override
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);
			context.write(key, result);
			}
		}

	
	
		public static class InterchangeMapper extends Mapper<Object, Text, IntWritable, Text>{

			public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
				String pair = itr.nextToken();
				String run=itr.nextToken();
				int run1=Integer.parseInt(run);
				run1=run1*(-1);
				context.write(new IntWritable(run1),new Text(pair));
			  }
		}

		public static class InterchangeReducer extends Reducer<IntWritable,Text,Text,IntWritable> {

	    		public void reduce(IntWritable key,Iterable<Text> values, Context context) throws IOException, InterruptedException {
	      			int k=key.get();
	      			k=k*(-1);
	      			for (Text val : values){
				context.write(new Text(val), new IntWritable(k));
	      			}

	    		}
	  	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
	    	Job job1 = Job.getInstance(conf1, "Batsman Vulnerability");
	    	job1.setJarByClass(BDAssign3.class);
	    	job1.setMapperClass(TokenizerMapper.class);
	    	job1.setCombinerClass(IntSumReducer.class);
	    	job1.setReducerClass(IntSumReducer.class);
	    	job1.setOutputKeyClass(Text.class);
	    	job1.setOutputValueClass(IntWritable.class);
	    	FileInputFormat.addInputPath(job1, new Path(args[0]));
	    	FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    	job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
         	Job job2 = Job.getInstance(conf2, "Descending Batsman Vul");
    		job2.setJarByClass(BDAssign3.class);
     		job2.setMapperClass(InterchangeMapper.class);
        	job2.setReducerClass(InterchangeReducer.class);
    		job2.setOutputKeyClass(Text.class);
    		job2.setOutputValueClass(IntWritable.class);
	 	job2.setMapOutputKeyClass(IntWritable.class);
    		job2.setMapOutputValueClass(Text.class);
    		FileInputFormat.addInputPath(job2, new Path(args[1]));
    		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}

	


