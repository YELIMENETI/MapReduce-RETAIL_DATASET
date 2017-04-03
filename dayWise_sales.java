import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// 12.TotalSales Based on Day(i.e Sunday ,Monday)-RetailData

public class dayWise_sales {
	public static class Mapper_Class extends
			Mapper<LongWritable, Text, Text, LongWritable> {

		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {

			try {
				String[] str = value.toString().split(";");
				String date1 = str[0];
				String reqDay = date(date1);
				long sales = Long.parseLong(str[8]);
				con.write(new Text(reqDay), new LongWritable(sales));

			} catch (Exception e) {
			}
		}

		public String date(String day) throws ParseException {
			SimpleDateFormat format1 = new SimpleDateFormat(
					"yyyy-mm-dd hh:mm:ss");
			Date dt1 = format1.parse(day);
			DateFormat format2 = new SimpleDateFormat("EEEE");
			String finalDay = format2.format(dt1);
			return finalDay;
		}
	}

	public static class Reducer_Class1 extends
			Reducer<Text, LongWritable, NullWritable, Text> {
		LongWritable sales_Percent = new LongWritable();
		private TreeMap<LongWritable, Text> repToRecord = new TreeMap<LongWritable, Text>();

		public void reduce(Text key, Iterable<LongWritable> values, Context con)
				throws IOException, InterruptedException {
			long sales = 0;
			for (LongWritable val : values) {
				sales = sales + val.get();
			}

			String myVal = key.toString();
			String totalSales = String.format("%d", sales);
			myVal = myVal + ',' + totalSales;

			repToRecord.put(new LongWritable(sales), new Text(myVal));
		}

		protected void cleanup(Context con) throws IOException,
				InterruptedException {
			for (Text t : repToRecord.values()) {
				con.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// conf.set("mapreduce.output.textoutputformat.separator",",");
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(dayWise_sales.class);
		job.setJobName("DateWise_Sales");
		job.setMapperClass(Mapper_Class.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(Reducer_Class1.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}