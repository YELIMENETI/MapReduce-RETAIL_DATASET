import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Avg for single product(i.e., amount/unit)-RetailData

public class avgcost_Prduct {
	public static class Mpercls extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			String[] str = value.toString().split(";");
			String prduct_id = str[5];
			String amount = str[7];
			String unit = str[6];
			String myval = amount + ',' + unit;
			con.write(new Text(prduct_id), new Text(myval));
		}
	}

	public static class reduceclass extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> value, Context con)
				throws IOException, InterruptedException {
			long Amount = 0;
			long unit = 0;
			double avg = 0.00;

			for (Text val : value) {
				String[] st = val.toString().split(",");
				Amount = Amount + Long.parseLong(st[0]);
				unit = unit + Long.parseLong(st[1]);
			}
			avg = Amount / unit;
			String AvgCost = String.format("%.2f%n", avg);
			con.write(key, new Text(AvgCost));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(avgcost_Prduct.class);
		job.setMapperClass(Mpercls.class);
		job.setReducerClass(reduceclass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}