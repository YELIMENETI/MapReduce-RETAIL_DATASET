import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class monthWiseSales {
	public static class mapreduce extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			try {
				String[] str = value.toString().split(";");

				String month = str[0];
				String Month=toMonth(month);
				String qty = str[6];
				String sales = str[8];

				String val = qty + ',' + sales;
				con.write(new Text(Month), new Text(val));
			} catch (Exception e) {
			}
		}
		public String toMonth(String mnth) throws ParseException {
			SimpleDateFormat format1 = new SimpleDateFormat(
					"yyyy-MM-dd hh:mm:ss");
			Date dt1 = format1.parse(mnth);
			DateFormat format2 = new SimpleDateFormat("MMMM");
			String finalDay = format2.format(dt1);
			return finalDay;
		}
	}

	public static class reduceclass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> value, Context con)
				throws IOException, InterruptedException {
			int qty = 0;
			int sales = 0;
			for (Text val : value) {
				String[] str = val.toString().split(",");
				qty += Integer.parseInt(str[0]);
				sales += Integer.parseInt(str[1]);
			}
			String Qty = String.format("%d", qty);
			String Sales = String.format("%d", sales);
			String myVal = Qty + ',' + Sales;
			con.write(key, new Text(myVal));
		}
	}

	public static void main(String[] args) throws IllegalArgumentException,
			IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(monthWiseSales.class);
		job.setMapperClass(mapreduce.class);
//		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(reduceclass.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
