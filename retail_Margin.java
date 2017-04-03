import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

//Margin for Single product

public class retail_Margin {
	public static class topproductmapper extends Mapper<LongWritable,Text,Text, Text>
	{
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String[] str=value.toString().split(";");
			String prdct_ID=str[5];
			String sales=str[8];
			String cost=str[7];
			String qty=str[6];
			String myvalue=qty+','+cost+','+sales;
			con.write(new Text(prdct_ID ),new Text(myvalue ));
		}
	}
	public static class reducerclass extends Reducer <Text,Text,NullWritable,Text>
	{
		private TreeMap<Double,Text> reptoRecord =new TreeMap<Double,Text>();
		public void reduce(Text key,Iterable<Text> value,Context con)
		{
			long totalcost=0;
			long totalqty=0;
			long totalsales=0;
			long profit=0;
			double margin=0.00;
			for(Text val:value )
			{
				String[] rec=val.toString().split(",");
				totalsales=totalsales+Long.valueOf(rec[2]);
				totalcost=totalcost+Long.valueOf(rec[1]);
				totalqty=totalqty+Long.valueOf(rec[0]);
			}
			profit= (totalsales-totalcost);
			if(totalcost!=0)
				margin=((totalsales-totalcost)*100)/totalcost;
			else 
				margin=((totalsales-totalcost)*100);
			
			String myValue = key.toString();
			String myProfit = String.format("%d", profit);
			String myMargin = String.format("%f", margin);
			String myQty = String.format("%d", totalqty);
			myValue = myValue+','+myProfit+','+myMargin+','+myQty ;
			reptoRecord.put(new Double(margin ),new Text(myValue));
		}
		protected void cleanup(Context con) throws IOException, InterruptedException {
			for(Text t:reptoRecord.values())
			{
				con.write(NullWritable.get(),t);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(retail_Margin.class);
		job.setMapperClass(topproductmapper .class);
		job.setReducerClass(reducerclass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}