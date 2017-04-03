import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Total No of customers based on ZipCodes

public class area_custCunt {

	public static class ageMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] str = value.toString().split(";");
			String Are_ZipCode = str[3];
			
			context.write(new Text(Are_ZipCode ), new Text(value));
		}
	}

	public static class age_partitionerclass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] rec = value.toString().split(";");
			String Are_zipcode = rec[3].trim();
			if (Are_zipcode.equals("A")) {
				return 0;
			} else if (Are_zipcode.equals("B")) {
				return 1;
			} else if (Are_zipcode.equals("C")) {
				return 2;
			} else if (Are_zipcode.equals("D")) {
				return 3;
			} else if (Are_zipcode.equals("E")) {
				return 4;
			} else {
				return 5;
			}
		}
	}

	public static class ageReducer extends
			Reducer<Text, Text, Text,LongWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long count=0;
			for(Text val:values)
			{
				String[] str=val.toString().split(";");
				String custId=str[1];
				count++;				
			}
			context.write(key,new LongWritable(count));
		}

				}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(area_custCunt.class);
		job.setMapperClass(ageMapper.class);
		job.setPartitionerClass(age_partitionerclass.class);
		
		job.setNumReduceTasks(6);
		job.setReducerClass(ageReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}