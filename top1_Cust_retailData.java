import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Top transcation for Product

public class top1_Cust_retailData {

	   public static class TopBuyerMapClass extends Mapper<LongWritable,Text,Text,LongWritable>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String prduct_id=str[5];
	            long amount = Long.parseLong(str[8]);  
	            context.write(new Text(prduct_id), new LongWritable(amount));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	   public static class TopBuyerReduceClass extends Reducer<Text,LongWritable,NullWritable,Text>
	   {
		   private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

	      public void reduce(Text key, Iterable <LongWritable> values, Context context) throws IOException, InterruptedException
	      {
	         long sum = 0;
	         String myValue = "";
	         String mySum = "";
	         for (LongWritable val : values)
	         {
	         	sum += val.get();
	         	 
	         }
	        myValue = key.toString();
	        mySum = String.format("%d", sum);
	        myValue = myValue + ',' + mySum;
			
	        repToRecordMap.put(new Long(sum), new Text(myValue));
			
			if (repToRecordMap.size() > 10) 
				{
						repToRecordMap.remove(repToRecordMap.firstKey());
				}
	      }      
			protected void cleanup(Context context) throws IOException,
			InterruptedException 
			{			
				for (Text t : repToRecordMap.values()) 
				{
						context.write(NullWritable.get(), t);
				}
			}	      
	   }
	   public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Top Buyer");
		    job.setJarByClass(top1_Cust_retailData.class);
		    job.setMapperClass(TopBuyerMapClass.class);
		    job.setReducerClass(TopBuyerReduceClass.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(LongWritable.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}