import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Top Products for AgeGroup & there Sales-RetailData

public class AgeWise_TopPrduct {

	public static class ageMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String myValue = "";
			String[] str = value.toString().split(";");
			String prduct_id = str[5];
			String Age = str[2];
			long sales = Long.parseLong(str[8]);
			myValue = Age + ',' + sales;
			context.write(new Text(prduct_id), new Text(myValue));
		}
	}

	public static class age_partitionerclass extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] rec = value.toString().split(",");
			String age = rec[0].trim();
			if (age.equals("A")) {
				return 0;
			} else if (age.equals("B")) {
				return 1;
			} else if (age.equals("C")) {
				return 2;
			} else if (age.equals("D")) {
				return 3;
			} else if (age.equals("E")) {
				return 4;
			} else if (age.equals("F")) {
				return 5;
			} else if (age.equals("G")) {
				return 6;
			} else if (age.equals("H")) {
				return 7;
			} else if (age.equals("I")) {
				return 8;
			} else {
				return 9;
			}
		}
	}

	public static class ageReducer extends
			Reducer<Text, Text, NullWritable, Text> {
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			long totalsales = 0;
			String Age = "";
			for (Text val : values) {
				String[] str = val.toString().split(",");
				totalsales = totalsales + Long.parseLong(str[1]);
				Age = str[0];
			}
			String myValue = key.toString();
			String mytotal = String.format("%d", totalsales);
			myValue = myValue + ',' + Age + ',' + mytotal;

			repToRecordMap.put(new Long(totalsales), new Text(myValue));
			if (repToRecordMap.size() > 5) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(AgeWise_TopPrduct.class);
		job.setMapperClass(ageMapper.class);
		job.setPartitionerClass(age_partitionerclass.class);
		job.setReducerClass(ageReducer.class);
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}