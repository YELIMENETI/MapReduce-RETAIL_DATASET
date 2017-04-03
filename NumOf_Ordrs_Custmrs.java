import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

//no of Orders & TotalSales for Customer-RetailData

public class NumOf_Ordrs_Custmrs {
	public static class mapclass extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] str=value.toString().split(";");
			String custId=str[1];
			String qty=str[6];
			String sales=str[8];
			String val=qty+','+sales;
			con.write(new Text(custId),new Text(val));			
		}
	}
	public static class reduceclass extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key,Iterable<Text> value,Context con) throws IOException, InterruptedException
		{
			int qty=0;
			int sales=0;
			for(Text val:value)
			{
				String[] str=val.toString().split(",");
				qty=qty+Integer.parseInt(str[0]);
				sales+=Integer.parseInt(str[1]);
			}
			String Qty=String.format("%d", qty);
			String Sales=String.format("%d", sales);
			String valOut=Qty+','+Sales;
			con.write(key, new Text(valOut));
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"");
		job.setJarByClass(NumOf_Ordrs_Custmrs.class);
		job.setMapperClass(mapclass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(reduceclass.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
